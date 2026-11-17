//! 全内存的共享状态：设备记录、每台设备的令牌、订阅者，以及防滥用所需的计数与限速。
//!
//! 进程重启后这些全部清空——设备是长连接，会自动重连并重新上报，令牌在重连时重新发一份
//! 给插件、插件再转交 APP。真要做多实例 / 重启不丢时，把这一层换成 Redis 即可（见 README）。

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use axum::{extract::ws::Message, http::HeaderMap};
use tokio::sync::{mpsc, RwLock};

use crate::model::{canonical, Address, AppOut};

/// 一条连接的出站通道。写线程独占 socket 的写端，其它地方往这里丢消息。
pub type Tx = mpsc::UnboundedSender<Message>;

/// 防滥用 / 容量的可调参数，全部有默认值，可用环境变量覆盖。
#[derive(Clone)]
pub struct Limits {
    /// 一条注册信息在「没有任何 APP 关注」后还能留多久（秒）。
    pub ttl_secs: u64,
    /// 设备记录总量上限。到线后靠淘汰「最久没被 APP 查」的那条腾位。
    pub max_devices: usize,
    /// 单个来源 IP 的并发连接上限（含设备与 APP）。
    pub max_conns_per_ip: u32,
    /// 全局新注册速率（每秒令牌数）与突发上限，用于挡住 CC 洪峰。
    pub reg_per_sec: f64,
    pub reg_burst: f64,
}

impl Limits {
    pub fn from_env() -> Self {
        fn u64v(k: &str, d: u64) -> u64 {
            std::env::var(k).ok().and_then(|s| s.trim().parse().ok()).unwrap_or(d)
        }
        fn usizev(k: &str, d: usize) -> usize {
            std::env::var(k).ok().and_then(|s| s.trim().parse().ok()).unwrap_or(d)
        }
        fn u32v(k: &str, d: u32) -> u32 {
            std::env::var(k).ok().and_then(|s| s.trim().parse().ok()).unwrap_or(d)
        }
        fn f64v(k: &str, d: f64) -> f64 {
            std::env::var(k).ok().and_then(|s| s.trim().parse().ok()).unwrap_or(d)
        }
        Self {
            ttl_secs: u64v("REGISTRATION_TTL_SECS", 900),
            max_devices: usizev("MAX_DEVICES", 100_000),
            max_conns_per_ip: u32v("MAX_CONNS_PER_IP", 50),
            reg_per_sec: f64v("REG_RATE_PER_SEC", 500.0),
            reg_burst: f64v("REG_RATE_BURST", 1000.0),
        }
    }
}

/// 令牌桶。只在内存里、按需补充，算出「这一发能不能放行」。
struct RateBucket {
    tokens: f64,
    last: Instant,
}

impl RateBucket {
    fn new(burst: f64) -> Self {
        Self {
            tokens: burst,
            last: Instant::now(),
        }
    }

    fn try_take(&mut self, per_sec: f64, burst: f64, now: Instant) -> bool {
        let dt = now.duration_since(self.last).as_secs_f64().max(0.0);
        self.tokens = (self.tokens + dt * per_sec).min(burst);
        self.last = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// 一台当前在线的设备。`conn_id` 用来分辨「这条连接还是不是当初登记的那条」——
/// 设备重连时新旧两条连接可能短暂并存，旧连接退出时不能把新记录删掉。
pub struct DeviceEntry {
    pub conn_id: u64,
    pub addresses: Vec<Address>,
    /// 最近一次被 APP 查询 / 订阅的时间（秒）。过期淘汰就按它算。
    pub last_app_interest: u64,
    /// 这条设备连接的出站通道，淘汰时用它把连接关掉、让插件重连补报。
    pub control: Tx,
}

/// 一次注册的处理结果。
pub enum RegisterOutcome {
    Ok {
        token: String,
        /// 地址相对上一次有没有变化（决定要不要通知订阅者）。
        changed: bool,
        /// 为腾位置被淘汰掉的旧连接（调用方负责关掉它们）。
        evicted: Vec<Tx>,
    },
    /// 触发全局注册速率上限。
    RateLimited,
    /// 记录已满且没有可淘汰的对象（每条都在被订阅）。
    Full,
}

pub struct Inner {
    /// 在线设备：instanceId → 记录。
    pub devices: HashMap<String, DeviceEntry>,
    /// 每台设备的令牌。**故意不随设备断开而删除**：设备重连要拿回同一个令牌，APP 手里的
    /// 才不会失效。进程重启清空，设备重连会换新令牌并重新同步给 APP。
    pub tokens: HashMap<String, String>,
    /// 订阅者：instanceId → (订阅 id → 出站通道)。
    pub watchers: HashMap<String, HashMap<u64, Tx>>,
    /// 每个来源 IP 当前的连接数。
    pub ip_conns: HashMap<String, u32>,
    pub next_conn: u64,
    pub next_watcher: u64,
    reg_bucket: RateBucket,
}

impl Inner {
    fn new(burst: f64) -> Self {
        Self {
            devices: HashMap::new(),
            tokens: HashMap::new(),
            watchers: HashMap::new(),
            ip_conns: HashMap::new(),
            next_conn: 0,
            next_watcher: 0,
            reg_bucket: RateBucket::new(burst),
        }
    }

    /// 收下一次设备注册：限速 → 满则淘汰「最久没人查」的 → 写记录、发令牌。
    pub fn register(
        &mut self,
        instance_id: &str,
        conn_id: u64,
        addresses: Vec<Address>,
        control: Tx,
        now: u64,
        limits: &Limits,
    ) -> RegisterOutcome {
        if !self
            .reg_bucket
            .try_take(limits.reg_per_sec, limits.reg_burst, Instant::now())
        {
            return RegisterOutcome::RateLimited;
        }

        let exists = self.devices.contains_key(instance_id);
        let mut evicted = Vec::new();
        if !exists && self.devices.len() >= limits.max_devices {
            // 只淘汰「没有活跃订阅」的记录；在它们之间挑「最久没被 APP 查」的。
            let victim = {
                let devices = &self.devices;
                let watchers = &self.watchers;
                devices
                    .iter()
                    .filter(|(id, _)| watchers.get(*id).map(|m| m.is_empty()).unwrap_or(true))
                    .min_by_key(|(_, e)| e.last_app_interest)
                    .map(|(id, _)| id.clone())
            };
            match victim {
                Some(vid) => {
                    if let Some(entry) = self.devices.remove(&vid) {
                        evicted.push(entry.control);
                    }
                }
                None => return RegisterOutcome::Full,
            }
        }

        let token = match self.tokens.get(instance_id) {
            Some(t) => t.clone(),
            None => {
                let t = uuid::Uuid::new_v4().to_string();
                self.tokens.insert(instance_id.to_string(), t.clone());
                t
            }
        };
        let changed = self
            .devices
            .get(instance_id)
            .map(|d| canonical(&d.addresses) != canonical(&addresses))
            .unwrap_or(true);
        self.devices.insert(
            instance_id.to_string(),
            DeviceEntry {
                conn_id,
                addresses,
                // 新注册给一整段宽限：从此刻起算，避免「刚注册就被下一轮清理扫掉」。
                last_app_interest: now,
                control,
            },
        );
        RegisterOutcome::Ok {
            token,
            changed,
            evicted,
        }
    }
}

#[derive(Clone)]
pub struct AppState {
    pub inner: Arc<RwLock<Inner>>,
    pub limits: Limits,
}

impl AppState {
    pub fn new(limits: Limits) -> Self {
        let burst = limits.reg_burst;
        Self {
            inner: Arc::new(RwLock::new(Inner::new(burst))),
            limits,
        }
    }
}

pub fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// 从代理头里取真实客户端 IP。流量过 CDN + 本机 nginx，优先取 `X-Forwarded-For` 最左段，
/// 其次 `X-Real-IP`；都取不到就归到 `unknown`（仅本机直连测试会出现）。
pub fn client_ip(headers: &HeaderMap) -> String {
    if let Some(xff) = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok()) {
        if let Some(first) = xff.split(',').next() {
            let s = first.trim();
            if !s.is_empty() {
                return s.to_string();
            }
        }
    }
    if let Some(xr) = headers.get("x-real-ip").and_then(|v| v.to_str().ok()) {
        let s = xr.trim();
        if !s.is_empty() {
            return s.to_string();
        }
    }
    "unknown".to_string()
}

/// 这个 IP 还有没有并发余量（只读检查，真正计数在 `ip_acquire`）。
pub async fn ip_has_room(state: &AppState, ip: &str) -> bool {
    let g = state.inner.read().await;
    g.ip_conns.get(ip).copied().unwrap_or(0) < state.limits.max_conns_per_ip
}

pub async fn ip_acquire(state: &AppState, ip: &str) {
    let mut g = state.inner.write().await;
    *g.ip_conns.entry(ip.to_string()).or_insert(0) += 1;
}

pub async fn ip_release(state: &AppState, ip: &str) {
    let mut g = state.inner.write().await;
    let empty = if let Some(c) = g.ip_conns.get_mut(ip) {
        *c = c.saturating_sub(1);
        *c == 0
    } else {
        false
    };
    if empty {
        g.ip_conns.remove(ip);
    }
}

/// 把这台设备当前的地址快照读出来，顺便刷新「被 APP 关注」的时间。
pub async fn snapshot(state: &AppState, instance_id: &str) -> (bool, Vec<Address>) {
    let mut g = state.inner.write().await;
    let now = now_secs();
    match g.devices.get_mut(instance_id) {
        Some(entry) => {
            entry.last_app_interest = now;
            (true, entry.addresses.clone())
        }
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

/// 定期清理：丢掉「超过 TTL 没有任何 APP 关注、且没有被订阅」的设备记录，并关掉它的连接，
/// 让插件走退避重连补报。返回这一轮清掉的数量（给日志用）。
pub async fn sweep(state: &AppState) -> usize {
    let now = now_secs();
    let ttl = state.limits.ttl_secs;
    let emptied: Vec<(String, Tx)> = {
        let mut g = state.inner.write().await;
        let expired: Vec<String> = {
            let devices = &g.devices;
            let watchers = &g.watchers;
            devices
                .iter()
                .filter(|(id, e)| {
                    let watched = watchers.get(*id).map(|m| !m.is_empty()).unwrap_or(false);
                    !watched && now.saturating_sub(e.last_app_interest) > ttl
                })
                .map(|(id, _)| id.clone())
                .collect()
        };
        let mut out = Vec::new();
        for id in expired {
            if let Some(entry) = g.devices.remove(&id) {
                out.push((id, entry.control));
            }
        }
        out
    };
    let count = emptied.len();
    for (_id, control) in emptied {
        let _ = control.send(Message::Close(None));
    }
    count
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits(max_devices: usize, reg_per_sec: f64, reg_burst: f64, ttl: u64) -> Limits {
        Limits {
            ttl_secs: ttl,
            max_devices,
            max_conns_per_ip: 50,
            reg_per_sec,
            reg_burst,
        }
    }

    fn addr(url: &str) -> Address {
        Address {
            kind: Some("relay".into()),
            url: url.into(),
        }
    }

    fn tx() -> Tx {
        let (tx, _rx) = mpsc::unbounded_channel::<Message>();
        // 故意泄漏接收端，测试里不关心出站消息是否被消费。
        std::mem::forget(_rx);
        tx
    }

    #[test]
    fn rate_limit_rejects_after_burst() {
        let l = limits(100, 0.0, 1.0, 60);
        let mut inner = Inner::new(1.0);
        let now = 1000;
        let r1 = inner.register("a", 1, vec![addr("wss://a")], tx(), now, &l);
        assert!(matches!(r1, RegisterOutcome::Ok { .. }));
        let r2 = inner.register("b", 2, vec![addr("wss://b")], tx(), now, &l);
        assert!(matches!(r2, RegisterOutcome::RateLimited));
    }

    #[test]
    fn over_capacity_evicts_least_recently_interested() {
        let l = limits(1, 1000.0, 1000.0, 60);
        let mut inner = Inner::new(1000.0);
        // A 更早注册（interest=100），B 的注册会把它挤掉。
        assert!(matches!(
            inner.register("a", 1, vec![addr("wss://a")], tx(), 100, &l),
            RegisterOutcome::Ok { .. }
        ));
        let r = inner.register("b", 2, vec![addr("wss://b")], tx(), 200, &l);
        match r {
            RegisterOutcome::Ok { evicted, .. } => assert_eq!(evicted.len(), 1),
            _ => panic!("expected ok"),
        }
        assert!(inner.devices.contains_key("b"));
        assert!(!inner.devices.contains_key("a"));
    }

    #[test]
    fn full_when_everything_is_watched() {
        let l = limits(1, 1000.0, 1000.0, 60);
        let mut inner = Inner::new(1000.0);
        assert!(matches!(
            inner.register("a", 1, vec![addr("wss://a")], tx(), 100, &l),
            RegisterOutcome::Ok { .. }
        ));
        // 给 A 挂一个订阅者，让它不可被淘汰。
        inner.watchers.entry("a".into()).or_default().insert(1, tx());
        let r = inner.register("b", 2, vec![addr("wss://b")], tx(), 200, &l);
        assert!(matches!(r, RegisterOutcome::Full));
    }

    #[tokio::test]
    async fn sweep_drops_idle_but_keeps_watched() {
        let l = limits(100, 1000.0, 1000.0, 5);
        let state = AppState::new(l);
        {
            let mut g = state.inner.write().await;
            g.register("idle", 1, vec![addr("wss://idle")], tx(), now_secs(), &state.limits);
            g.register("used", 2, vec![addr("wss://used")], tx(), now_secs(), &state.limits);
            // 两条都划到「很久以前」，超过 5 秒 TTL。
            let old = now_secs() - 100;
            g.devices.get_mut("idle").unwrap().last_app_interest = old;
            g.devices.get_mut("used").unwrap().last_app_interest = old;
            // used 有人订阅，不该被清。
            g.watchers.entry("used".into()).or_default().insert(1, tx());
        }
        let removed = sweep(&state).await;
        assert_eq!(removed, 1);
        let g = state.inner.read().await;
        assert!(!g.devices.contains_key("idle"));
        assert!(g.devices.contains_key("used"));
    }
}
