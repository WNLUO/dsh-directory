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
    let started = std::time::Instant::now();

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
    #[allow(unused_assignments)]
    let mut reason: &'static str = "closed";

    // 空闲长连接需要心跳：设备注册后平时不发数据，前面的 CDN 会按空闲把连接掐掉，
    // 设备于是不断重连、握手 QPS 虚高。每 20 秒发一次 Ping，标准客户端会自动回 Pong。
    loop {
        let msg = match tokio::time::timeout(std::time::Duration::from_secs(20), stream.next()).await {
            Ok(Some(Ok(m))) => m,
            Ok(Some(Err(_))) => {
                reason = "peer_err";
                break;
            }
            Ok(None) => {
                reason = "peer_eof";
                break;
            }
            Err(_) => {
                if tx.send(Message::Ping(Vec::new().into())).is_err() {
                    reason = "hb_send_fail";
                    break;
                }
                continue;
            }
        };
        let text = match msg {
            Message::Text(t) => t.to_string(),
            Message::Ping(p) => {
                let _ = tx.send(Message::Pong(p));
                continue;
            }
            Message::Close(_) => {
                reason = "client_close";
                break;
            }
            _ => continue,
        };
        match serde_json::from_str::<DeviceIn>(&text) {
            Ok(DeviceIn::Register { payload }) => {
                if payload.len() > MAX_PAYLOAD_BYTES {
                    send_json(&tx, &DeviceOut::Error { message: "payload too large".into() });
                    reason = "payload_too_large";
                    break;
                }
                let now = now_secs();
                let limits = state.limits.clone();
                let had_existing = {
                    let g = state.inner.read().await;
                    g.devices.contains_key(&instance_id)
                };
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
                        eprintln!(
                            "diag register id={} ip={} outcome=ok replaced={} token={}",
                            instance_id,
                            ip,
                            had_existing,
                            provided_token.is_some()
                        );
                    }
                    RegisterOutcome::RateLimited => {
                        eprintln!("diag register id={} ip={} outcome=rate_limited", instance_id, ip);
                        send_json(&tx, &DeviceOut::Error { message: "rate limited, retry later".into() });
                        reason = "rate_limited";
                        break;
                    }
                    RegisterOutcome::Full => {
                        eprintln!("diag register id={} ip={} outcome=server_full", instance_id, ip);
                        send_json(&tx, &DeviceOut::Error { message: "server busy, retry later".into() });
                        reason = "server_full";
                        break;
                    }
                    RegisterOutcome::Unauthorized => {
                        eprintln!(
                            "diag register id={} ip={} outcome=unauthorized token={}",
                            instance_id,
                            ip,
                            provided_token.is_some()
                        );
                        // 明确的「身份冲突」而不是笼统的 unauthorized：这个 id 已经有别的令牌在用了
                        // （同一 instanceId 被多台机器/多进程共用，或本机令牌丢了）。客户端据此走
                        // 「换身份或长退避」而不是当成一次性网络错误重试。
                        send_json(&tx, &DeviceOut::Error { message: "identity_conflict".into() });
                        reason = "unauthorized";
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
    let (removed, superseded) = {
        let mut g = state.inner.write().await;
        let current = g.devices.get(&instance_id).map(|d| d.conn_id);
        let owned = current == Some(conn_id);
        if owned {
            g.devices.remove(&instance_id);
        }
        (owned, registered && current.is_some() && current != Some(conn_id))
    };
    drop(tx);
    let _ = writer.await;
    if removed {
        broadcast_offline(&state, &instance_id).await;
    }
    eprintln!(
        "diag conn_end id={} ip={} dur_ms={} registered={} superseded={} reason={}",
        instance_id,
        ip,
        started.elapsed().as_millis(),
        registered,
        superseded,
        if superseded { "superseded" } else { reason }
    );
    ip_release(&state, &ip).await;
}

fn send_json(tx: &Tx, out: &DeviceOut) {
    if let Ok(payload) = serde_json::to_string(out) {
        let _ = tx.send(Message::Text(payload.into()));
    }
}
