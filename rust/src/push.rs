//! 苹果离线推送：总控持一把 APNs 密钥，替所有实例给它们的手机发通知。
//!
//! 为什么在这里而不是中转：中转是「一人一台」（还有大量自建），密钥配在那儿等于每台都得配一遍；
//! 总控是全项目唯一一台共用的服务器，配一次所有中转都通。
//!
//! 鉴权用 **token-based**（`.p8` 密钥 + provider JWT），不用证书：密钥不过期，一把能推该团队
//! 全部 App。三样配置：`APNS_KEY`（.p8 内容或路径）、`APNS_KEY_ID`、`APNS_TEAM_ID`，外加
//! `APNS_TOPIC`（App 的 bundle id，默认 `com.dsh.mobile`）。**缺任何一样，推送就是关的**——
//! 没配密钥的部署照常跑地址目录，只是不发通知。
//!
//! 环境：开发构建拿到的令牌属于沙盒，TestFlight / App Store 的属于生产，两端点不通用。令牌
//! 登记时没带环境就按生产记；真发的时候若苹果回 `BadDeviceToken`，换另一个端点再试一次，
//! 成功就把这台设备的环境改记成那一头——开发者拿手机自测不用手工切环境。
//!
//! 刻意不做的事：不重试（苹果没收到就等下一次提醒；推送是增强不是唯一通道），只在令牌
//! 永久失效时摘掉那条记录。reqwest 的连接池负责复用连接。
//!
//! 总控在这里能看到通知文字（会话名、审批摘要、提问原文）——苹果那边必须收到明文，谁发推送
//! 谁就得看见内容。地址那部分仍然是密文，总控照样解不开。

use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::{
    extract::{DefaultBodyLimit, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use p256::ecdsa::signature::Signer;
use p256::ecdsa::{Signature, SigningKey};
use p256::pkcs8;
use serde_json::{json, Value};

use crate::push_store::PushEntry;
use crate::state::{now_secs, AppState};

/// 推送两个入口的正文上限：一条通知就是标题 + 正文 + 一小段交互数据。
const MAX_BODY_BYTES: usize = 16 * 1024;

/// 每实例的发送速率：一分钟 60 条、突发 20 条。总控是公网入口，这条口子连着苹果的接口，
/// 不能让人拿着自己那枚令牌无上限地刷。
pub(crate) const PUSH_PER_SEC: f64 = 1.0;
pub(crate) const PUSH_BURST: f64 = 20.0;

/// APNs 生产 / 沙盒入口。令牌跟环境绑定，打错端点会回 `BadDeviceToken`。
const APNS_PRODUCTION: &str = "https://api.push.apple.com";
const APNS_SANDBOX: &str = "https://api.sandbox.push.apple.com";

/// 默认通知的 topic 就是 App 的 bundle id。
const DEFAULT_TOPIC: &str = "com.dsh.mobile";

/// 一条载荷要守的字节预算。APNs 硬上限 4096，留出余量：超过就先降级再发。
const PAYLOAD_BUDGET: usize = 3800;

/// 一份可用的推送配置。四样齐全才算配置过。
#[derive(Clone)]
pub struct ApnsConfig {
    pub team_id: String,
    pub key_id: String,
    pub topic: String,
    private_key: Arc<SigningKey>,
}

impl ApnsConfig {
    /// 从部署配置装配。`key` 接受 PEM 内容或文件路径（含 `~` 展开）。
    /// 任一要素缺失 / 密钥解析失败都返回 `Err`，调用方据此判定推送关闭。
    pub fn from_env() -> Result<Self, String> {
        let team_id = non_empty("APNS_TEAM_ID")?;
        let key_id = non_empty("APNS_KEY_ID")?;
        let key = non_empty("APNS_KEY")?;
        let topic = std::env::var("APNS_TOPIC")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| DEFAULT_TOPIC.to_string());
        Self::from_parts(&team_id, &key_id, &topic, &key)
    }

    /// 四要素显式装配（测试用它避开环境变量的全局性）。
    fn from_parts(team_id: &str, key_id: &str, topic: &str, key: &str) -> Result<Self, String> {
        let pem = load_pem(key)?;
        let der = pem_to_der(&pem)?;
        let private_key = signing_key_from_der(&der)?;
        Ok(Self {
            team_id: team_id.to_string(),
            key_id: key_id.to_string(),
            topic: topic.to_string(),
            private_key: Arc::new(private_key),
        })
    }

    /// 签一份 provider JWT。每次发送都现签（30 分钟内苹果都认，签一次是微秒级，
    /// 不值得为缓存引入时钟账）。
    fn provider_jwt(&self) -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let header = json!({ "alg": "ES256", "kid": self.key_id });
        let claims = json!({ "iss": self.team_id, "iat": now });
        let encode = |value: &Value| base64_url(serde_json::to_string(value).unwrap_or_default().as_bytes());
        let signing_input = format!("{}.{}", encode(&header), encode(&claims));
        let signature: Signature = self.private_key.sign(signing_input.as_bytes());
        format!("{signing_input}.{}", base64_url(&signature.to_bytes()))
    }
}

#[derive(Debug)]
pub struct SendError {
    message: String,
    /// 这条令牌永久失效（苹果说它不认），该从表里摘掉。
    invalid_token: bool,
    /// 410：令牌已经注销，换环境也没用。
    unregistered: bool,
}

impl std::fmt::Display for SendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

fn rejected(status: u16, body: &str) -> SendError {
    let reason = serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|value| value.get("reason").and_then(Value::as_str).map(str::to_string));
    let unregistered = status == 410;
    let invalid_token = unregistered
        || matches!(reason.as_deref(), Some("BadDeviceToken" | "ExpiredToken"));
    SendError {
        message: format!("apns rejected ({status}): {body}"),
        invalid_token,
        unregistered,
    }
}

/// 推送出口。`None` = 推送未配置（功能关闭）。
#[derive(Clone)]
pub struct ApnsClient {
    config: ApnsConfig,
    http: reqwest::Client,
}

impl ApnsClient {
    pub fn new(config: ApnsConfig) -> Self {
        Self {
            config,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("reqwest client with timeout"),
        }
    }

    /// 发一条推送。失败交给调用方记日志；推送是增强通道，不影响地址目录。
    pub async fn send(&self, token: &str, env: &str, payload: &Value) -> Result<(), SendError> {
        let jwt = self.config.provider_jwt();
        let response = self
            .http
            .post(format!("{}/3/device/{}", endpoint(env), token))
            .header(header::AUTHORIZATION, format!("bearer {jwt}"))
            // 令牌鉴权必须自带 topic（App 的 bundle id），否则苹果回 MissingTopic。
            .header("apns-topic", self.config.topic.as_str())
            .header("apns-push-type", "alert")
            .header("apns-priority", "10")
            .json(payload)
            .send()
            .await
            .map_err(|error| SendError {
                message: format!("apns request failed: {error}"),
                invalid_token: false,
                unregistered: false,
            })?;
        let status = response.status();
        if status.is_success() {
            Ok(())
        } else {
            let body = response.text().await.unwrap_or_default();
            Err(rejected(status.as_u16(), &body))
        }
    }
}

/// 全局出口。启动时配置过才有；其余全部路径先问它。
static CLIENT: OnceLock<Option<ApnsClient>> = OnceLock::new();

/// 装配全局推送出口。配置缺失 / 密钥解析失败都只是打一条日志，**不崩**：
/// 没配密钥的部署照常提供地址目录。
pub fn init_from_env() {
    let client = match ApnsConfig::from_env() {
        Ok(config) => {
            println!("dsh-directory: push enabled, topic={}", config.topic);
            Some(ApnsClient::new(config))
        }
        Err(reason) => {
            println!("dsh-directory: push disabled: {reason}");
            None
        }
    };
    let _ = CLIENT.set(client);
}

/// 推送开关是否打开（健康检查露出用）。
pub fn enabled() -> bool {
    CLIENT.get().map(|client| client.is_some()).unwrap_or(false)
}

fn client() -> Option<ApnsClient> {
    CLIENT.get().and_then(|client| client.as_ref().cloned())
}

/// 令牌登记的环境。只有这两个值，别的一律按生产记。
fn normalize_env(value: Option<&str>) -> &'static str {
    match value.map(str::trim) {
        Some("sandbox") => "sandbox",
        _ => "production",
    }
}

fn other_env(env: &str) -> &'static str {
    if env == "sandbox" {
        "production"
    } else {
        "sandbox"
    }
}

fn endpoint(env: &str) -> &'static str {
    if env == "sandbox" {
        APNS_SANDBOX
    } else {
        APNS_PRODUCTION
    }
}

/// 目标令牌是否该发：空 `device_id` 命中该实例全部令牌，非空时只命中同名设备。
pub fn should_send(token_device: &str, device_id: &str) -> bool {
    device_id.is_empty() || token_device == device_id
}

/// 组装推送载荷。`category` 为空不写 aps.category，`interaction` 为 None 不写顶层
/// `dshInteraction`——普通完成提醒的载荷因此没有多余的键。
fn build_payload(
    instance_id: &str,
    title: &str,
    body: &str,
    session_id: &str,
    category: &str,
    interaction: Option<&Value>,
) -> Value {
    let mut aps = json!({
        "alert": { "title": title, "body": body },
        "sound": "default",
    });
    if !category.is_empty() {
        aps["category"] = json!(category);
    }
    let mut payload = json!({
        "aps": aps,
        // 自定义键：App 点通知跳会话用。
        "sessionId": session_id,
        "instanceId": instance_id,
    });
    if let Some(interaction) = interaction {
        payload["dshInteraction"] = interaction.clone();
    }
    payload
}

/// 载荷字节数（APNs 按这份 JSON 计）。
fn payload_size(payload: &Value) -> usize {
    serde_json::to_vec(payload).map(|bytes| bytes.len()).unwrap_or(0)
}

/// 载荷瘦身：超预算先丢交互数据，再丢类别，最后截正文——只有这一条受影响。
///
/// 降级顺序有讲究：交互数据最贵（审批选项整份塞在里面），类别丢了顶多少几个通知按钮，
/// 正文丢了就只是一条普通提醒。都不改全局，下一条照常完整发。
fn fit_payload(payload: &mut Value) {
    if payload_size(payload) <= PAYLOAD_BUDGET {
        return;
    }
    if let Some(map) = payload.as_object_mut() {
        map.remove("dshInteraction");
    }
    if payload_size(payload) <= PAYLOAD_BUDGET {
        return;
    }
    if let Some(aps) = payload.get_mut("aps").and_then(Value::as_object_mut) {
        aps.remove("category");
    }
    if payload_size(payload) <= PAYLOAD_BUDGET {
        return;
    }
    // 还超就只能是标题 / 正文太长（工具输出整段贴进通知文本）。转义会放大字节数，
    // 所以退着砍而不是按比例一次砍到位。
    if truncate_field(payload, "body") || payload_size(payload) > PAYLOAD_BUDGET {
        truncate_field(payload, "title");
    }
}

/// 把 `aps.alert.<field>` 反复砍到四分之三，直到载荷进预算。已经砍空返回 true。
fn truncate_field(payload: &mut Value, field: &str) -> bool {
    loop {
        let mut text = payload["aps"]["alert"][field]
            .as_str()
            .unwrap_or("")
            .to_string();
        if text.is_empty() || payload_size(payload) <= PAYLOAD_BUDGET {
            return text.is_empty();
        }
        let mut keep = text.len() * 3 / 4;
        while keep > 0 && !text.is_char_boundary(keep) {
            keep -= 1;
        }
        text.truncate(keep);
        payload["aps"]["alert"][field] = Value::String(text);
    }
}

fn non_empty(name: &str) -> Result<String, String> {
    match std::env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(value.trim().to_string()),
        _ => Err(format!("{name} not set")),
    }
}

/// `.p8` 来源：内容（以 `-----BEGIN` 开头）或文件路径（支持 `~/` 展开）。
fn load_pem(key: &str) -> Result<String, String> {
    if key.starts_with("-----BEGIN") {
        return Ok(key.to_string());
    }
    let path = if let Some(rest) = key.strip_prefix("~/") {
        let home =
            std::env::var("HOME").map_err(|_| "APNS_KEY 带了 ~ 但拿不到 HOME".to_string())?;
        std::path::PathBuf::from(home).join(rest)
    } else {
        std::path::PathBuf::from(key)
    };
    std::fs::read_to_string(&path)
        .map_err(|error| format!("APNS_KEY 文件读不到（{}）：{error}", path.display()))
}

fn base64_url(bytes: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// 剥 PEM 壳：取 `-----BEGIN … KEY-----` 与 `-----END … KEY-----` 之间的 base64，
/// 拼起来解成 DER。只认标准 p8 的 PKCS#8 形状。
fn pem_to_der(pem: &str) -> Result<Vec<u8>, String> {
    use base64::Engine as _;
    let body = pem
        .lines()
        .filter(|line| !line.starts_with("-----") && !line.trim().is_empty())
        .collect::<String>();
    base64::engine::general_purpose::STANDARD
        .decode(body.as_bytes())
        .map_err(|error| format!("APNS_KEY 的 base64 段解不开：{error}"))
}

fn signing_key_from_der(der: &[u8]) -> Result<SigningKey, String> {
    SigningKey::try_from(
        pkcs8::PrivateKeyInfo::try_from(der)
            .map_err(|error| format!("APNS_KEY 不是可解析的 ES256 私钥：{error}"))?,
    )
    .map_err(|error| format!("APNS_KEY 不是可解析的 ES256 私钥：{error}"))
}

/// 两个入口。挂在总控同一个端口上，反代前缀（`/findme/`）由部署侧统一加。
pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/push/token", post(token_handler))
        .route("/push/send", post(send_handler))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
}

fn reply(status: StatusCode, body: Value) -> Response {
    (status, Json(body)).into_response()
}

fn bearer(headers: &HeaderMap) -> String {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .unwrap_or("")
        .trim()
        .to_string()
}

fn instance_of(headers: &HeaderMap) -> String {
    headers
        .get("x-dsh-instance")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .trim()
        .to_string()
}

/// 两个入口共用一把锁：就是设备连总控时用的那枚实例令牌（APP 从配对拿到，插件落在本地）。
/// 总控本来就不校验「你是不是那台设备」，只校验你拿得出那台设备的令牌——与查询地址同一条口径。
async fn authorized(state: &AppState, headers: &HeaderMap) -> Result<String, Response> {
    let instance_id = instance_of(headers);
    let token = bearer(headers);
    if instance_id.is_empty() || token.is_empty() {
        return Err(reply(
            StatusCode::UNAUTHORIZED,
            json!({ "ok": false, "error": "instance token required" }),
        ));
    }
    let ok = state
        .inner
        .read()
        .await
        .verify_instance_token(&instance_id, &token);
    if !ok {
        return Err(reply(
            StatusCode::FORBIDDEN,
            json!({ "ok": false, "error": "bad instance token" }),
        ));
    }
    Ok(instance_id)
}

/// `POST /push/token`：APP 拿到 APNs 令牌就打这里。正文
/// `{deviceId, token, platform?, env?}`。
async fn token_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let instance_id = match authorized(&state, &headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let device_id = body
        .get("deviceId")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_string();
    let token = body
        .get("token")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_string();
    if device_id.is_empty() || token.is_empty() {
        return reply(
            StatusCode::BAD_REQUEST,
            json!({ "ok": false, "error": "deviceId and token are required" }),
        );
    }
    let platform = body
        .get("platform")
        .and_then(Value::as_str)
        .unwrap_or("ios")
        .trim()
        .to_string();
    let env = normalize_env(body.get("env").and_then(Value::as_str)).to_string();
    let entry = PushEntry {
        token,
        platform,
        env,
        updated_at: now_secs(),
    };
    let mut inner = state.inner.write().await;
    if let Err(error) = inner.upsert_push_token(&instance_id, &device_id, entry, &state.limits) {
        eprintln!("push store: write failed: {error}");
        return reply(
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({ "ok": false, "error": "store unavailable" }),
        );
    }
    reply(StatusCode::OK, json!({ "ok": true }))
}

/// `POST /push/send`：插件在任务收尾 / 待处理卡片出现时打这里。正文
/// `{title, body?, sessionId?, pushKind?, category?, interaction?, deviceId?}`。
///
/// `deviceId` 留空 = 推给这台实例登记过的全部手机。响应只说明「收到几条目标」，
/// 真正的发送在后台跑：苹果那边的时延不该让宿主等。
async fn send_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let instance_id = match authorized(&state, &headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let title = body
        .get("title")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_string();
    if title.is_empty() {
        return reply(
            StatusCode::BAD_REQUEST,
            json!({ "ok": false, "error": "title is required" }),
        );
    }
    let device_id = body
        .get("deviceId")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_string();
    let session_id = body
        .get("sessionId")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let category = body
        .get("category")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let interaction = body.get("interaction").cloned();

    let mut inner = state.inner.write().await;
    if !inner.push_allowed(&instance_id) {
        return reply(
            StatusCode::TOO_MANY_REQUESTS,
            json!({ "ok": false, "error": "too many pushes" }),
        );
    }
    let targets = inner.push_targets_of(&instance_id, &device_id);
    drop(inner);

    if targets.is_empty() {
        return reply(StatusCode::OK, json!({ "ok": true, "targets": 0 }));
    }
    let mut payload = build_payload(
        &instance_id,
        &title,
        body.get("body").and_then(Value::as_str).unwrap_or(""),
        &session_id,
        &category,
        interaction.as_ref(),
    );
    fit_payload(&mut payload);
    let targets_len = targets.len();
    let task_state = state.clone();
    tokio::spawn(async move {
        dispatch(&task_state, &instance_id, payload, targets).await;
    });
    reply(
        StatusCode::OK,
        json!({ "ok": true, "targets": targets_len }),
    )
}

/// 逐台发。同一份载荷，只有令牌和环境不同。
async fn dispatch(
    state: &AppState,
    instance_id: &str,
    payload: Value,
    targets: Vec<(String, PushEntry)>,
) {
    let Some(client) = client() else {
        return;
    };
    for (device_id, entry) in targets {
        match client.send(&entry.token, &entry.env, &payload).await {
            Ok(()) => {}
            Err(error) => {
                // 环境猜错（开发版令牌打到生产入口）就换另一头再试一次，成功就改记。
                // 410 是令牌注销了，换环境也一样，直接摘。
                if error.invalid_token && !error.unregistered {
                    let other = other_env(&entry.env);
                    if client.send(&entry.token, other, &payload).await.is_ok() {
                        let mut inner = state.inner.write().await;
                        if let Err(store_error) = inner.set_push_env(
                            instance_id,
                            &device_id,
                            &entry.token,
                            other,
                            &state.limits,
                        ) {
                            eprintln!("push store: env update failed: {store_error}");
                        }
                        continue;
                    }
                }
                if error.invalid_token {
                    let mut inner = state.inner.write().await;
                    inner.delete_push_token_if_matches(instance_id, &device_id, &entry.token);
                }
                eprintln!(
                    "push: send failed instance={instance_id} device={device_id}: {error}"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 一把真实的 ES256 测试私钥（生成后写死在这里；它从未在苹果侧注册过，
    /// 推不出任何真实通知）。
    const TEST_KEY: &str = "-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgFg5UYiqkYR5BnCoT
QndCKqnRh0+VOmdhExtEZrVj9TChRANCAARTsnSS77gadbKPl8KC7k3Anwzawv3n
ZatyTBhq0hz791WjzJSgGlDy4f2G6ebtWFSuiyyAY8TM4RUUQFuy8ygC
-----END PRIVATE KEY-----";

    #[test]
    fn only_permanent_device_errors_invalidate_tokens() {
        for (status, reason) in [
            (410, "Unregistered"),
            (400, "BadDeviceToken"),
            (400, "ExpiredToken"),
        ] {
            let error = rejected(status, &json!({ "reason": reason }).to_string());
            assert!(error.invalid_token, "{status} {reason}");
        }
        for (status, reason) in [
            (403, "ExpiredProviderToken"),
            (403, "InvalidProviderToken"),
            (429, "TooManyRequests"),
            (500, "InternalServerError"),
        ] {
            assert!(!rejected(status, &json!({ "reason": reason }).to_string()).invalid_token);
        }
        // 410 一定是「这条令牌没了」，换环境也不用再试。
        assert!(rejected(410, "not json").unregistered);
        assert!(!rejected(400, &json!({ "reason": "BadDeviceToken" }).to_string()).unregistered);
    }

    #[test]
    fn parses_inline_pem_and_signs() {
        let config =
            ApnsConfig::from_parts("TEAM", "KEYID", "com.dsh.mobile", TEST_KEY).expect("测试密钥应可解析");
        let jwt = config.provider_jwt();
        assert_eq!(jwt.matches('.').count(), 2, "{jwt}");
    }

    #[test]
    fn config_requires_all_three_parts() {
        std::env::remove_var("APNS_TEAM_ID");
        std::env::remove_var("APNS_KEY_ID");
        std::env::remove_var("APNS_KEY");
        assert!(ApnsConfig::from_env().is_err(), "缺配置必须判推送关闭");
    }

    #[test]
    fn garbage_key_is_rejected_instead_of_panicking() {
        assert!(ApnsConfig::from_parts("T", "K", "topic", "not-a-key").is_err());
    }

    #[test]
    fn inline_pem_is_accepted_as_content() {
        assert!(load_pem(TEST_KEY).unwrap().starts_with("-----BEGIN"));
    }

    #[test]
    fn environments_map_to_endpoints_and_switch_once() {
        assert_eq!(endpoint("sandbox"), APNS_SANDBOX);
        assert_eq!(endpoint("production"), APNS_PRODUCTION);
        // 认不出的环境值按生产走：宁可多试一次沙盒，也别把生产令牌打到沙盒。
        assert_eq!(normalize_env(Some("whatever")), "production");
        assert_eq!(normalize_env(None), "production");
        assert_eq!(normalize_env(Some("sandbox")), "sandbox");
        assert_eq!(other_env("production"), "sandbox");
        assert_eq!(other_env("sandbox"), "production");
    }

    /// 空 deviceId 是「推给该实例全部令牌」，不能被当成无效目标丢掉；
    /// 非空时只命中同名设备。
    #[test]
    fn empty_device_id_targets_all_tokens() {
        assert!(should_send("dev-a", ""));
        assert!(should_send("dev-b", ""));
        assert!(should_send("dev-a", "dev-a"));
        assert!(!should_send("dev-a", "dev-b"));
    }

    /// 富推送：category 进 aps，interaction 原样进顶层 dshInteraction，
    /// 基础字段一个不少。
    #[test]
    fn payload_carries_category_and_interaction() {
        let interaction = json!({ "kind": "approval", "id": "req-1", "options": ["yes", "no"] });
        let payload = build_payload("inst-1", "请审批", "工具调用", "sess-9", "dsh.approval", Some(&interaction));
        assert_eq!(payload["aps"]["category"], "dsh.approval");
        assert_eq!(payload["aps"]["sound"], "default");
        assert_eq!(payload["aps"]["alert"]["title"], "请审批");
        assert_eq!(payload["aps"]["alert"]["body"], "工具调用");
        assert_eq!(payload["dshInteraction"], interaction);
        assert_eq!(payload["sessionId"], "sess-9");
        assert_eq!(payload["instanceId"], "inst-1");
    }

    /// 普通完成提醒不传 category / interaction：载荷里不该冒出空 category 键。
    #[test]
    fn payload_omits_empty_category_and_interaction() {
        let payload = build_payload("inst-1", "完成", "done", "sess-9", "", None);
        assert!(payload["aps"].get("category").is_none(), "空 category 不许写出");
        assert!(payload.get("dshInteraction").is_none());
        assert_eq!(payload["aps"]["alert"]["title"], "完成");
        assert_eq!(payload["sessionId"], "sess-9");
        assert_eq!(payload["instanceId"], "inst-1");
    }

    /// 载荷超预算时依次丢 dshInteraction、aps.category，最终仍在 APNs 上限内。
    #[test]
    fn oversized_payload_degrades_until_it_fits() {
        let interaction = json!({ "kind": "approval", "blob": "x".repeat(5000) });
        let mut payload = build_payload("i", "审批", "工具调用", "s", "dsh.approval", Some(&interaction));
        assert!(payload_size(&payload) > 4096);
        fit_payload(&mut payload);
        assert!(payload_size(&payload) < 4096);
        assert!(payload.get("dshInteraction").is_none(), "先丢交互数据");
        assert_eq!(payload["aps"]["category"], "dsh.approval", "丢完就够，不必再丢类别");
    }

    /// 类别也丢了还不够就砍正文，且不砍坏多字节字符。
    #[test]
    fn oversized_body_is_truncated_on_char_boundary() {
        let mut payload = build_payload("i", "完成", &"工".repeat(3000), "s", "dsh.done", None);
        assert!(payload_size(&payload) > 4096);
        fit_payload(&mut payload);
        assert!(payload_size(&payload) < 4096);
        let body = payload["aps"]["alert"]["body"].as_str().unwrap();
        assert!(body.chars().all(|c| c == '工'), "截断不能切出半个字符");
    }

    /// 够小的载荷一个字节都不动。
    #[test]
    fn small_payload_is_untouched() {
        let mut payload = build_payload(
            "i",
            "完成",
            "done",
            "s",
            "dsh.done",
            Some(&json!({ "kind": "approval" })),
        );
        let before = serde_json::to_string(&payload).unwrap();
        fit_payload(&mut payload);
        assert_eq!(serde_json::to_string(&payload).unwrap(), before);
    }
}
