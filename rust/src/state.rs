//! 目录状态与注册鉴权：有效令牌重连不占新注册配额，静默死链按单调期限回收。
//!
//! 设备记录是不透明密文；重启后等待设备重新上报，不恢复过期的在线地址。
//! 未授权注册（令牌不符 / 缺令牌）按来源 IP 限流：少量突发后长时间才放行一次，
//! 抑制单个 IP 反复猜令牌或重放已登记的 instanceId。

use std::{
    collections::HashMap,
    io,
    path::Path,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{extract::ws::Message, http::HeaderMap};
use tokio::sync::{mpsc, RwLock};
use futures_util::{Stream, StreamExt};

use crate::model::{AppOut, DeviceOut};
use crate::token_store::{TokenEntry, TokenStore};

/// 一条连接的出站通道。写线程独占 socket 的写端，其它地方往这里丢消息。
pub type Tx = mpsc::UnboundedSender<Message>;

pub const WS_PING_INTERVAL: Duration = Duration::from_secs(20);
pub const WS_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Ping 只保活代理；入站帧才延长活跃期限，排入写队列不算对端活着。
pub async fn receive_ws<S, E>(
    stream: &mut S, tx: &Tx, last_received: &mut tokio::time::Instant,
    ping_interval: Duration, idle_timeout: Duration,
) -> Result<Message, &'static str>
where S: Stream<Item = Result<Message, E>> + Unpin {
    loop {
        let deadline = *last_received + idle_timeout;
        let next_ping = (tokio::time::Instant::now() + ping_interval).min(deadline);
        match tokio::time::timeout_at(next_ping, stream.next()).await {
            Ok(Some(Ok(message))) => {
                *last_received = tokio::time::Instant::now();
                return Ok(message);
            }
            Ok(Some(Err(_))) => return Err("peer_err"),
            Ok(None) => return Err("peer_eof"),
            Err(_) if tokio::time::Instant::now() >= deadline => return Err("hb_timeout"),
            Err(_) => {
                if tx.send(Message::Ping(Vec::new().into())).is_err() { return Err("hb_send_fail"); }
            }
        }
    }
}

/// 给已排队的错误/关闭帧一个短收尾窗口，阻塞写端不能无限拖住离线和配额回收。
pub async fn stop_writer(mut writer: tokio::task::JoinHandle<()>) {
    if tokio::time::timeout(Duration::from_millis(250), &mut writer).await.is_err() {
        writer.abort();
        let _ = writer.await;
    }
}

/// 防滥用 / 容量的可调参数，全部有默认值，可用环境变量覆盖。
#[derive(Clone)]
pub struct Limits {
    /// 设备记录总量上限。到线后靠淘汰「最久没被 APP 查」的那条腾位。
    pub max_devices: usize,
    /// 每台设备的令牌记录上限。到线后淘汰最久没来注册的（挡住「不停换 instanceId」把内存吃满）。
    pub max_tokens: usize,
    /// 单个来源 IP 的并发连接上限（含设备与 APP）。
    pub max_conns_per_ip: u32,
    /// 全局新注册速率（每秒令牌数）与突发上限，用于挡住 CC 洪峰。
    pub reg_per_sec: f64,
    pub reg_burst: f64,
    /// 单个来源 IP 的未授权注册失败额度（突发次数）与恢复间隔（秒）。
    /// 额度用尽后，该 IP 每 `reg_fail_refill_secs` 秒才恢复一次尝试机会。
    pub reg_fail_burst: f64,
    pub reg_fail_refill_secs: f64,
}

impl Limits {
    pub fn from_env() -> Self {
        fn usizev(k: &str, d: usize) -> usize {
            std::env::var(k)
                .ok()
                .and_then(|s| s.trim().parse().ok())
                .unwrap_or(d)
        }
        fn u32v(k: &str, d: u32) -> u32 {
            std::env::var(k)
                .ok()
                .and_then(|s| s.trim().parse().ok())
                .unwrap_or(d)
        }
        fn f64v(k: &str, d: f64) -> f64 {
            std::env::var(k)
                .ok()
                .and_then(|s| s.trim().parse().ok())
                .unwrap_or(d)
        }
        Self {
            max_devices: usizev("MAX_DEVICES", 100_000),
            max_tokens: usizev("MAX_TOKENS", 200_000),
            max_conns_per_ip: u32v("MAX_CONNS_PER_IP", 50),
            reg_per_sec: f64v("REG_RATE_PER_SEC", 500.0),
            reg_burst: f64v("REG_RATE_BURST", 1000.0),
            reg_fail_burst: f64v("REG_FAIL_BURST", 5.0),
            reg_fail_refill_secs: f64v("REG_FAIL_REFILL_SECS", 300.0),
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
        self.refill(per_sec, burst, now);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    /// 只按经过的时间补令牌到上限，不消耗。用于「当前是否够一次」的只读判定。
    fn refill(&mut self, per_sec: f64, burst: f64, now: Instant) {
        let dt = now.duration_since(self.last).as_secs_f64().max(0.0);
        self.tokens = (self.tokens + dt * per_sec).min(burst);
        self.last = now;
    }
}

fn refill_per_sec(limits: &Limits) -> f64 {
    if limits.reg_fail_refill_secs > 0.0 {
        1.0 / limits.reg_fail_refill_secs
    } else {
        0.0
    }
}

/// 一台当前在线的设备。`conn_id` 用来分辨「这条连接还是不是当初登记的那条」——
/// 设备重连时新旧两条连接可能短暂并存，旧连接退出时不能把新记录删掉。
pub struct DeviceEntry {
    pub conn_id: u64,
    /// 不透明密文载荷（信封 JSON）。总控不解析。
    pub payload: String,
    /// 最近一次被 APP 查询 / 订阅的时间（秒）。满载淘汰就按它挑「最久没人查」的。
    pub last_app_interest: u64,
    /// 这条设备连接的出站通道。
    pub control: Tx,
}

/// 一位正在等自检结果的 APP：那条连接的出站通道，以及它在自己连接内的编号
/// （断开时按编号摘，别把下一位的等待位误删）。
pub struct DiagnoseWaiter {
    pub id: u64,
    pub tx: Tx,
}

/// 一次注册的处理结果。
pub enum RegisterOutcome {
    Ok {
        token: String,
        /// 为腾位置被淘汰掉的旧连接（调用方负责关掉它们）。
        evicted: Vec<Tx>,
    },
    /// 触发全局注册速率上限。
    RateLimited,
    /// 记录已满且没有可淘汰的对象（每条都在被订阅）。
    Full,
    /// 这台设备已经登记过令牌，但这次没带上同一枚 —— 视为冒充，拒绝。
    Unauthorized,
    /// 令牌未能持久化，不能向客户端承诺注册成功。
    StorageUnavailable,
}

pub struct Inner {
    /// 在线设备：instanceId → 记录。
    pub devices: HashMap<String, DeviceEntry>,
    /// 每台设备的令牌。**故意不随设备断开而删除**：设备重连要拿回同一个令牌，APP 手里的
    /// 才不会失效。超上限时按最久未注册淘汰。
    pub tokens: HashMap<String, TokenEntry>,
    /// 订阅者：instanceId → (订阅 id → 出站通道)。
    pub watchers: HashMap<String, HashMap<u64, Tx>>,
    /// 正在等自检结果的 APP：instanceId → 那一条连接。只留一位 —— 同一台设备同一时刻
    /// 只会有一个「自检」在跑，后问的顶掉先问的（先问的那条自己会超时）。
    pub diagnose_waiters: HashMap<String, DiagnoseWaiter>,
    /// 每个来源 IP 当前的连接数。
    pub ip_conns: HashMap<String, u32>,
    pub next_conn: u64,
    pub next_watcher: u64,
    reg_bucket: RateBucket,
    /// 每个来源 IP 的未授权注册失败额度桶。只有鉴权失败才扣，成功注册不扣。
    fail_buckets: HashMap<String, RateBucket>,
    token_store: Option<TokenStore>,
}

impl Inner {
    fn new(burst: f64) -> Self {
        Self {
            devices: HashMap::new(),
            tokens: HashMap::new(),
            watchers: HashMap::new(),
            diagnose_waiters: HashMap::new(),
            ip_conns: HashMap::new(),
            next_conn: 0,
            next_watcher: 0,
            reg_bucket: RateBucket::new(burst),
            fail_buckets: HashMap::new(),
            token_store: None,
        }
    }

    /// 旧连接退出不能清掉已经接替它的新地址。
    pub fn disconnect_device(&mut self, instance_id: &str, conn_id: u64, registered: bool) -> (bool, bool) {
        let current = self.devices.get(instance_id).map(|device| device.conn_id);
        let owned = current == Some(conn_id);
        if owned {
            self.devices.remove(instance_id);
            self.broadcast_device(instance_id, AppOut::Offline { instance_id: instance_id.to_string() });
            // 还在等自检的那一位别干等到超时：设备这条腿断了，答案已经不可能回来。
            if let Some(waiter) = self.diagnose_waiters.remove(instance_id) {
                let event = AppOut::DiagnosisUnavailable {
                    instance_id: instance_id.to_string(),
                    reason: "device_offline".to_string(),
                };
                if let Ok(text) = serde_json::to_string(&event) {
                    let _ = waiter.tx.send(Message::Text(text.into()));
                }
            }
        }
        (owned, registered && current.is_some() && current != Some(conn_id))
    }

    /// 调用方持状态锁，消息直接入订阅队列，不在变更与事件之间让出执行权。
    fn broadcast_device(&self, instance_id: &str, event: AppOut) {
        let Ok(payload) = serde_json::to_string(&event) else { return };
        if let Some(watchers) = self.watchers.get(instance_id) {
            for sender in watchers.values() { let _ = sender.send(Message::Text(payload.clone().into())); }
        }
    }

    /// 把「请自检」转给这台设备此刻那条连接，并记住问话的那一位。
    ///
    /// 设备不在总控上（没连、或那条连接的写端已经没了）返回 false，调用方据此当场回
    /// `diagnosis_unavailable`，别让手机干等一次超时。
    pub fn ask_device(&mut self, instance_id: &str, asker_id: u64, asker: Tx) -> bool {
        let ask = match serde_json::to_string(&DeviceOut::Ask) {
            Ok(text) => text,
            Err(_) => return false,
        };
        let Some(entry) = self.devices.get_mut(instance_id) else {
            return false;
        };
        if entry.control.send(Message::Text(ask.into())).is_err() {
            return false;
        }
        self.diagnose_waiters.insert(
            instance_id.to_string(),
            DiagnoseWaiter { id: asker_id, tx: asker },
        );
        true
    }

    /// 设备回了自检结果：只交给当前这条设备连接登记的那位等待者，并把等待位清掉。
    ///
    /// 已经没人等（APP 关掉了弹窗 / 换了另一台）就丢掉这份答案，不回错 —— 设备那边
    /// 没有理由为「对方走了」重发。
    pub fn answer_diagnosis(&mut self, instance_id: &str, conn_id: u64, payload: String) -> bool {
        if self.devices.get(instance_id).map(|device| device.conn_id) != Some(conn_id) {
            return false;
        }
        let Some(waiter) = self.diagnose_waiters.remove(instance_id) else {
            return false;
        };
        let event = AppOut::Diagnosis {
            instance_id: instance_id.to_string(),
            payload,
        };
        match serde_json::to_string(&event) {
            Ok(text) => waiter.tx.send(Message::Text(text.into())).is_ok(),
            Err(_) => false,
        }
    }

    /// APP 那条连接断了：它要是还在等结果，把等待位摘掉。
    pub fn drop_diagnose_waiter(&mut self, instance_id: &str, asker_id: u64) {
        if self.diagnose_waiters.get(instance_id).map(|waiter| waiter.id) == Some(asker_id) {
            self.diagnose_waiters.remove(instance_id);
        }
    }

    /// 返回本次成功注册后需要淘汰的旧令牌，持久化失败时不改现有鉴权。
    fn token_victims(&self, instance_id: &str, max: usize) -> Vec<String> {
        let total = self.tokens.len() + usize::from(!self.tokens.contains_key(instance_id));
        if total <= max.max(1) {
            return Vec::new();
        }
        let mut entries: Vec<(&str, u64)> = self
            .tokens
            .iter()
            .filter(|(id, _)| id.as_str() != instance_id)
            .map(|(id, entry)| (id.as_str(), entry.last_seen))
            .collect();
        entries.sort_by_key(|(id, last)| (*last, *id));
        let remove = (entries.len() + 1).saturating_sub(max.max(1));
        entries
            .into_iter()
            .take(remove)
            .map(|(id, _)| id.to_string())
            .collect()
    }

    /// 这个来源 IP 的未授权注册额度是否已耗尽（耗尽 = 当前应直接拒，先别握手）。
    ///
    /// 只读判定：按经过时间把令牌补满到上限后再看够不够一次；没有失败记录就不拦。
    pub fn reg_fail_throttled(&self, ip: &str, limits: &Limits, now: Instant) -> bool {
        let per_sec = refill_per_sec(limits);
        let burst = limits.reg_fail_burst.max(1.0);
        match self.fail_buckets.get(ip) {
            Some(bucket) => {
                let dt = now.duration_since(bucket.last).as_secs_f64().max(0.0);
                let projected = (bucket.tokens + dt * per_sec).min(burst);
                projected < 1.0
            }
            None => false,
        }
    }

    /// 记一次未授权注册失败：扣掉该 IP 一次额度。额度已空也保持为空，靠时间慢慢恢复。
    pub fn record_reg_fail(&mut self, ip: &str, limits: &Limits, now: Instant) {
        let per_sec = refill_per_sec(limits);
        let burst = limits.reg_fail_burst.max(1.0);
        let bucket = self
            .fail_buckets
            .entry(ip.to_string())
            .or_insert_with(|| RateBucket::new(burst));
        let _ = bucket.try_take(per_sec, burst, now);
    }

    /// 定期清理已长期无活动的失败桶，避免被轮换 IP 撑大内存。
    pub fn sweep_reg_fails(&mut self, limits: &Limits, now: Instant) {
        let per_sec = refill_per_sec(limits);
        if per_sec <= 0.0 {
            self.fail_buckets.clear();
            return;
        }
        // 从空额度恢复满需要的时长 + 10 分钟缓冲；超过这个时间没再失败就清掉。
        let keep_idle =
            Duration::from_secs_f64(limits.reg_fail_burst.max(1.0) / per_sec + 600.0);
        self.fail_buckets
            .retain(|_, bucket| now.duration_since(bucket.last) < keep_idle);
    }

    /// 收下一次设备注册：校验令牌 → 新身份限速 → 容量与持久化 → 发布在线记录。
    pub fn register(
        &mut self,
        instance_id: &str,
        conn_id: u64,
        payload: String,
        control: Tx,
        provided_token: Option<&str>,
        now: u64,
        limits: &Limits,
    ) -> RegisterOutcome {
        // 授权：已登记过令牌的设备，重连/覆盖时必须带同一枚；否则当成冒充拒绝。
        // 本地令牌表没有记录才签发新令牌，不采信客户端自带的未知令牌。
        let token = match self.tokens.get(instance_id) {
            Some(entry) => match provided_token {
                Some(p) if p == entry.token => entry.token.clone(),
                _ => return RegisterOutcome::Unauthorized,
            },
            None => {
                if !self.reg_bucket.try_take(limits.reg_per_sec, limits.reg_burst, Instant::now()) {
                    return RegisterOutcome::RateLimited;
                }
                uuid::Uuid::new_v4().to_string()
            }
        };

        let exists = self.devices.contains_key(instance_id);
        let victim = if !exists && self.devices.len() >= limits.max_devices {
            let candidate = self
                .devices
                .iter()
                .filter(|(id, _)| self.watchers.get(*id).map(|m| m.is_empty()).unwrap_or(true))
                .min_by_key(|(_, entry)| entry.last_app_interest)
                .map(|(id, _)| id.clone());
            match candidate {
                Some(id) => Some(id),
                None => return RegisterOutcome::Full,
            }
        } else {
            None
        };
        let entry = TokenEntry {
            token: token.clone(),
            last_seen: now,
        };
        let removed = self.token_victims(instance_id, limits.max_tokens);
        if let Some(store) = &mut self.token_store {
            if let Err(error) = store.record(instance_id, &entry, removed.clone()) {
                eprintln!("token store: registration write failed: {error}");
                return RegisterOutcome::StorageUnavailable;
            }
        }
        self.tokens.insert(instance_id.to_string(), entry);
        for id in removed {
            self.tokens.remove(&id);
        }
        if let Some(store) = &mut self.token_store {
            if let Err(error) = store.compact_if_needed(&self.tokens) {
                eprintln!("token store: compaction failed: {error}");
            }
        }
        let mut evicted = Vec::new();
        if let Some(entry) = victim.and_then(|id| self.devices.remove(&id)) {
            evicted.push(entry.control);
        }
        self.devices.insert(
            instance_id.to_string(),
            DeviceEntry {
                conn_id,
                payload,
                last_app_interest: now,
                control,
            },
        );
        RegisterOutcome::Ok { token, evicted }
    }
}

#[derive(Clone)]
pub struct AppState {
    pub inner: Arc<RwLock<Inner>>,
    pub limits: Limits,
}

impl AppState {
    pub fn open(limits: Limits, token_path: &Path) -> io::Result<Self> {
        let (store, tokens) = TokenStore::open(token_path)?;
        let mut inner = Inner::new(limits.reg_burst);
        inner.tokens = tokens;
        inner.token_store = Some(store);
        Ok(Self {
            inner: Arc::new(RwLock::new(inner)),
            limits,
        })
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

/// 这个来源 IP 的未授权注册额度是否已耗尽（耗尽则该次握手直接 429）。
pub async fn reg_fail_throttled(state: &AppState, ip: &str) -> bool {
    let g = state.inner.read().await;
    g.reg_fail_throttled(ip, &state.limits, Instant::now())
}

/// 记一次未授权注册失败，扣该 IP 一次额度。
pub async fn record_reg_fail(state: &AppState, ip: &str) {
    let mut g = state.inner.write().await;
    g.record_reg_fail(ip, &state.limits, Instant::now());
}

/// 把这台设备当前的密文载荷读出来，顺便刷新「被 APP 关注」的时间。
pub async fn snapshot(state: &AppState, instance_id: &str) -> (bool, Option<String>) {
    let mut g = state.inner.write().await;
    let now = now_secs();
    match g.devices.get_mut(instance_id) {
        Some(entry) => {
            entry.last_app_interest = now;
            (true, Some(entry.payload.clone()))
        }
        None => (false, None),
    }
}

/// 载荷有变化时，只通知正在订阅这台设备的 APP。没订阅者就什么都不做（链路自然静默）。
pub async fn broadcast_update(state: &AppState, instance_id: &str, payload: &str) {
    let g = state.inner.read().await;
    g.broadcast_device(instance_id, AppOut::Update {
        instance_id: instance_id.to_string(), payload: payload.to_string(),
    });
}

#[cfg(test)]
#[path = "state_tests.rs"]
mod tests;
