//! 全内存的共享状态：设备记录、每台设备的令牌、以及「谁在订阅哪台设备」。
//!
//! 进程重启后这些全部清空——设备是长连接，会自动重连并重新上报，令牌在重连时重新发一份
//! 给插件、插件再转交 APP。真要做多实例 / 重启不丢时，把这一层换成 Redis 即可（见 README）。

use std::{
    collections::HashMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::extract::ws::Message;
use tokio::sync::{mpsc, RwLock};

use crate::model::{Address, AppOut};

/// 一条 APP 连接的出站通道。写线程独占 socket 的写端，其它地方往这里丢消息。
pub type Tx = mpsc::UnboundedSender<Message>;

/// 一台当前在线的设备。`conn_id` 用来分辨「这条连接还是不是当初登记的那条」——
/// 设备重连时新旧两条连接可能短暂并存，旧连接退出时不能把新记录删掉。
pub struct DeviceEntry {
    pub conn_id: u64,
    pub addresses: Vec<Address>,
    pub updated_at: u64,
}

#[derive(Default)]
pub struct Inner {
    /// 在线设备：instanceId → 记录。
    pub devices: HashMap<String, DeviceEntry>,
    /// 每台设备的令牌。**故意不随设备断开而删除**：设备重连要拿回同一个令牌，APP 手里的
    /// 才不会失效。进程重启清空，设备重连会换新令牌并重新同步给 APP。
    pub tokens: HashMap<String, String>,
    /// 订阅者：instanceId → (订阅 id → 出站通道)。
    pub watchers: HashMap<String, HashMap<u64, Tx>>,
    pub next_conn: u64,
    pub next_watcher: u64,
}

#[derive(Clone)]
pub struct AppState {
    pub inner: Arc<RwLock<Inner>>,
    /// 设备注册用的部署级口令；未配置表示不校验（本机 / 测试）。
    pub enroll_secret: Option<String>,
}

impl AppState {
    pub fn new(enroll_secret: Option<String>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner::default())),
            enroll_secret,
        }
    }
}

pub fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// 把这台设备当前的地址快照读出来。
pub async fn snapshot(state: &AppState, instance_id: &str) -> (bool, Vec<Address>) {
    let g = state.inner.read().await;
    match g.devices.get(instance_id) {
        Some(entry) => (true, entry.addresses.clone()),
        None => (false, Vec::new()),
    }
}

/// 地址有变化时，只通知正在订阅这台设备的 APP。没订阅者就什么都不做（链路自然静默）。
pub async fn broadcast_update(state: &AppState, instance_id: &str, addresses: &[Address]) {
    let payload = match serde_json::to_string(&AppOut::Update {
        instance_id: instance_id.to_string(),
        addresses: addresses.to_vec(),
    }) {
        Ok(p) => p,
        Err(_) => return,
    };
    let txs: Vec<Tx> = {
        let g = state.inner.read().await;
        g.watchers
            .get(instance_id)
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default()
    };
    for tx in txs {
        let _ = tx.send(Message::Text(payload.clone().into()));
    }
}

/// 设备断开时通知订阅者。
pub async fn broadcast_offline(state: &AppState, instance_id: &str) {
    let payload = match serde_json::to_string(&AppOut::Offline {
        instance_id: instance_id.to_string(),
    }) {
        Ok(p) => p,
        Err(_) => return,
    };
    let txs: Vec<Tx> = {
        let g = state.inner.read().await;
        g.watchers
            .get(instance_id)
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default()
    };
    for tx in txs {
        let _ = tx.send(Message::Text(payload.clone().into()));
    }
}
