//! 设备侧长连接（`/ws/device`）。
//!
//! 设备（插件宿主）主动连上来，报 `instanceId` 与一组连接地址；连接一断，这台就算离线，
//! 订阅它的 APP 会收到一条离线事件。地址没变化时这里不发任何主动消息，链路保持静默。

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Query, State,
    },
    http::StatusCode,
    response::{IntoResponse, Response},
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;

use crate::model::{canonical, normalize_addresses, DeviceIn, DeviceOut};
use crate::state::{broadcast_offline, broadcast_update, AppState, DeviceEntry, Tx};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceQuery {
    instance_id: String,
    #[serde(default)]
    secret: Option<String>,
}

pub async fn handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Query(query): Query<DeviceQuery>,
) -> Response {
    if let Some(expected) = &state.enroll_secret {
        if query.secret.as_deref() != Some(expected.as_str()) {
            return (StatusCode::UNAUTHORIZED, "invalid enrollment secret").into_response();
        }
    }
    let instance_id = query.instance_id;
    ws.on_upgrade(move |socket| handle(socket, state, instance_id))
}

async fn handle(socket: WebSocket, state: AppState, instance_id: String) {
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
            Ok(DeviceIn::Register { addresses }) => {
                let addresses = normalize_addresses(addresses);
                let (token, changed) = {
                    let mut g = state.inner.write().await;
                    let token = match g.tokens.get(&instance_id) {
                        Some(t) => t.clone(),
                        None => {
                            let t = uuid::Uuid::new_v4().to_string();
                            g.tokens.insert(instance_id.clone(), t.clone());
                            t
                        }
                    };
                    let changed = g
                        .devices
                        .get(&instance_id)
                        .map(|d| canonical(&d.addresses) != canonical(&addresses))
                        .unwrap_or(true);
                    g.devices.insert(
                        instance_id.clone(),
                        DeviceEntry {
                            conn_id,
                            addresses: addresses.clone(),
                            updated_at: crate::state::now_secs(),
                        },
                    );
                    (token, changed)
                };
                send_json(&tx, &DeviceOut::Registered {
                    instance_id: instance_id.clone(),
                    token,
                });
                if changed {
                    broadcast_update(&state, &instance_id, &addresses).await;
                }
                registered = true;
            }
            Ok(DeviceIn::Update { addresses }) => {
                if !registered {
                    send_json(&tx, &DeviceOut::Error {
                        message: "send register first".into(),
                    });
                    continue;
                }
                let addresses = normalize_addresses(addresses);
                let changed = {
                    let mut g = state.inner.write().await;
                    match g.devices.get(&instance_id) {
                        Some(d) if d.conn_id == conn_id => {
                            let changed = canonical(&d.addresses) != canonical(&addresses);
                            if changed {
                                if let Some(entry) = g.devices.get_mut(&instance_id) {
                                    entry.addresses = addresses.clone();
                                    entry.updated_at = crate::state::now_secs();
                                }
                            }
                            changed
                        }
                        _ => false,
                    }
                };
                send_json(&tx, &DeviceOut::Updated);
                if changed {
                    broadcast_update(&state, &instance_id, &addresses).await;
                }
            }
            Err(_) => {
                send_json(&tx, &DeviceOut::Error {
                    message: "bad message".into(),
                });
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
}

fn send_json(tx: &Tx, out: &DeviceOut) {
    if let Ok(payload) = serde_json::to_string(out) {
        let _ = tx.send(Message::Text(payload.into()));
    }
}
