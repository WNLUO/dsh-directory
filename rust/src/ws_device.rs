//! 设备侧长连接（`/ws/device`）。
//!
//! 设备（插件宿主）主动连上来，报 `instanceId` 与一份**加密载荷**（全部连接地址封在里面，
//! 总控不解析）；连接一断，这台就算离线，订阅它的 APP 会收到一条离线事件。载荷没变化时
//! 这里不发任何主动消息，链路保持静默。

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Query, State,
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;

use crate::model::{DeviceIn, DeviceOut, MAX_PAYLOAD_BYTES};
use crate::state::{
    broadcast_offline, broadcast_update, client_ip, ip_acquire, ip_has_room, ip_release, now_secs,
    AppState, RegisterOutcome, Tx,
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceQuery {
    instance_id: String,
    /// 这台设备上次注册时拿到的令牌。首次注册为空；之后重连/覆盖必须带上同一枚（防冒充）。
    #[serde(default)]
    token: Option<String>,
}

pub async fn handler(
    ws: WebSocketUpgrade,
    headers: HeaderMap,
    State(state): State<AppState>,
    Query(query): Query<DeviceQuery>,
) -> Response {
    let ip = client_ip(&headers);
    if !ip_has_room(&state, &ip).await {
        return (StatusCode::TOO_MANY_REQUESTS, "too many connections").into_response();
    }
    let DeviceQuery { instance_id, token } = query;
    ws.on_upgrade(move |socket| handle(socket, state, instance_id, ip, token))
}

async fn handle(
    socket: WebSocket,
    state: AppState,
    instance_id: String,
    ip: String,
    provided_token: Option<String>,
) {
    ip_acquire(&state, &ip).await;

    let (mut sink, mut stream) = socket.split();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Message>();
    let writer = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if sink.send(msg).await.is_err() {
                break;
            }
        }
    });

    let conn_id = {
        let mut g = state.inner.write().await;
        g.next_conn += 1;
        g.next_conn
    };
    let mut registered = false;

    while let Some(next) = stream.next().await {
        let msg = match next {
            Ok(m) => m,
            Err(_) => break,
        };
        let text = match msg {
            Message::Text(t) => t.to_string(),
            Message::Ping(p) => {
                let _ = tx.send(Message::Pong(p));
                continue;
            }
            Message::Close(_) => break,
            _ => continue,
        };
        match serde_json::from_str::<DeviceIn>(&text) {
            Ok(DeviceIn::Register { payload }) => {
                if payload.len() > MAX_PAYLOAD_BYTES {
                    send_json(&tx, &DeviceOut::Error { message: "payload too large".into() });
                    break;
                }
                let now = now_secs();
                let limits = state.limits.clone();
                let outcome = {
                    let mut g = state.inner.write().await;
                    g.register(
                        &instance_id,
                        conn_id,
                        payload.clone(),
                        tx.clone(),
                        provided_token.as_deref(),
                        now,
                        &limits,
                    )
                };
                match outcome {
                    RegisterOutcome::Ok { token, evicted } => {
                        for control in evicted {
                            let _ = control.send(Message::Close(None));
                        }
                        send_json(
                            &tx,
                            &DeviceOut::Registered {
                                instance_id: instance_id.clone(),
                                token,
                            },
                        );
                        broadcast_update(&state, &instance_id, &payload).await;
                        registered = true;
                    }
                    RegisterOutcome::RateLimited => {
                        send_json(&tx, &DeviceOut::Error { message: "rate limited, retry later".into() });
                        break;
                    }
                    RegisterOutcome::Full => {
                        send_json(&tx, &DeviceOut::Error { message: "server busy, retry later".into() });
                        break;
                    }
                    RegisterOutcome::Unauthorized => {
                        send_json(&tx, &DeviceOut::Error { message: "unauthorized".into() });
                        break;
                    }
                }
            }
            Ok(DeviceIn::Update { payload }) => {
                if !registered {
                    send_json(&tx, &DeviceOut::Error { message: "send register first".into() });
                    continue;
                }
                if payload.len() > MAX_PAYLOAD_BYTES {
                    send_json(&tx, &DeviceOut::Error { message: "payload too large".into() });
                    continue;
                }
                let owned = {
                    let mut g = state.inner.write().await;
                    match g.devices.get_mut(&instance_id) {
                        Some(entry) if entry.conn_id == conn_id => {
                            entry.payload = payload.clone();
                            true
                        }
                        _ => false,
                    }
                };
                if owned {
                    send_json(&tx, &DeviceOut::Updated);
                    broadcast_update(&state, &instance_id, &payload).await;
                } else {
                    // 记录已被淘汰 / 换了一条连接：让设备重新注册。
                    send_json(&tx, &DeviceOut::Error { message: "send register first".into() });
                }
            }
            Err(_) => {
                send_json(&tx, &DeviceOut::Error { message: "bad message".into() });
            }
        }
    }

    // 连接结束：只有「登记这条记录的仍是当前连接」时才删除并广播离线，避免重连竞态里
    // 旧连接把新记录清掉。
    let removed = {
        let mut g = state.inner.write().await;
        let owned = g
            .devices
            .get(&instance_id)
            .map(|d| d.conn_id == conn_id)
            .unwrap_or(false);
        if owned {
            g.devices.remove(&instance_id);
        }
        owned
    };
    drop(tx);
    let _ = writer.await;
    if removed {
        broadcast_offline(&state, &instance_id).await;
    }
    ip_release(&state, &ip).await;
}

fn send_json(tx: &Tx, out: &DeviceOut) {
    if let Ok(payload) = serde_json::to_string(out) {
        let _ = tx.send(Message::Text(payload.into()));
    }
}
