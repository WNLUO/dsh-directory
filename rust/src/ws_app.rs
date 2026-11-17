//! APP 侧长连接（`/ws/app`）。
//!
//! APP 拿着插件转交的令牌连上来，可以查一次（`query`），也可以订阅（`watch`）——
//! 订阅后先给一份当前快照，之后只在地址变化 / 设备掉线时才收到推送，平时一个字都不发。

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

use crate::model::{AppIn, AppOut};
use crate::state::{snapshot, AppState, Tx};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppQuery {
    instance_id: String,
    token: String,
}

pub async fn handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Query(query): Query<AppQuery>,
) -> Response {
    let authorized = {
        let g = state.inner.read().await;
        g.tokens
            .get(&query.instance_id)
            .map(|t| t == &query.token)
            .unwrap_or(false)
    };
    if !authorized {
        return (StatusCode::UNAUTHORIZED, "invalid token").into_response();
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

    let watcher_id = {
        let mut g = state.inner.write().await;
        g.next_watcher += 1;
        g.next_watcher
    };
    let mut watching = false;

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
        match serde_json::from_str::<AppIn>(&text) {
            Ok(AppIn::Query) => {
                let (online, addresses) = snapshot(&state, &instance_id).await;
                send_snapshot(&tx, &instance_id, online, addresses);
            }
            Ok(AppIn::Watch) => {
                // 注册订阅与读取当前快照在同一把写锁里完成：中间不会有更新漏掉。
                let (online, addresses) = {
                    let mut g = state.inner.write().await;
                    g.watchers
                        .entry(instance_id.clone())
                        .or_default()
                        .insert(watcher_id, tx.clone());
                    match g.devices.get(&instance_id) {
                        Some(entry) => (true, entry.addresses.clone()),
                        None => (false, Vec::new()),
                    }
                };
                watching = true;
                send_snapshot(&tx, &instance_id, online, addresses);
            }
            Ok(AppIn::Unwatch) => {
                if watching {
                    let mut g = state.inner.write().await;
                    if let Some(m) = g.watchers.get_mut(&instance_id) {
                        m.remove(&watcher_id);
                        if m.is_empty() {
                            g.watchers.remove(&instance_id);
                        }
                    }
                    watching = false;
                }
            }
            Err(_) => {
                send_json(&tx, &AppOut::Error {
                    message: "bad message".into(),
                });
            }
        }
    }

    if watching {
        let mut g = state.inner.write().await;
        if let Some(m) = g.watchers.get_mut(&instance_id) {
            m.remove(&watcher_id);
            if m.is_empty() {
                g.watchers.remove(&instance_id);
            }
        }
    }
    drop(tx);
    let _ = writer.await;
}

fn send_snapshot(tx: &Tx, instance_id: &str, online: bool, addresses: Vec<crate::model::Address>) {
    send_json(
        tx,
        &AppOut::Snapshot {
            instance_id: instance_id.to_string(),
            online,
            addresses,
        },
    );
}

fn send_json(tx: &Tx, out: &AppOut) {
    if let Ok(payload) = serde_json::to_string(out) {
        let _ = tx.send(Message::Text(payload.into()));
    }
}
