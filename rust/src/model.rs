//! 线上协议的数据形状（设备↔总控、APP↔总控）。
//!
//! **总控是盲的**：设备上报的是一段用「实例历史密钥（HK）」加出来的密文信封（`payload`），
//! 总控不解析、不持钥、解不开。设备把手上所有连接地址（中转 / 局域网 / IPv6 / 直连）封进
//! 这段载荷里；APP 用自己那份 HK 解开。总控只负责按 instanceId 存取这段不透明字符串。

use serde::{Deserialize, Serialize};

/// 单次上报载荷的上限（密文信封 JSON 的字节数）。超了直接拒，避免被塞超大 blob。
pub const MAX_PAYLOAD_BYTES: usize = 64 * 1024;

/// 设备 → 总控。
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DeviceIn {
    /// 首次上线 / 重连后上报整份加密载荷。
    Register { payload: String },
    /// 载荷变化时上报新的整份加密载荷（不是增量）。
    Update { payload: String },
    /// 回应 APP 的「自检」：一份密文答案（信封 JSON），总控不解析。
    Answer { payload: String },
}

/// 总控 → 设备。
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DeviceOut {
    /// 注册成功，带回这台设备的令牌。插件留着它，交给 APP。
    Registered { instance_id: String, token: String },
    /// 一次更新已被收下。
    Updated,
    /// 有 APP 请这台设备做一次自检。**不带任何内容**：总控不告诉设备是谁问的，也不需要。
    Ask,
    Error { message: String },
}

/// APP → 总控。
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AppIn {
    /// 查一次当前载荷（一次性问答）。
    Query,
    /// 订阅这台设备：先回一份当前快照，之后只在有变化时推送。
    Watch,
    /// 取消订阅。
    Unwatch,
    /// 请这台设备做一次自检。总控只负责转达：不记录是谁问的，也不指定答案给谁
    /// （答案只回到这一条连接上，见 `ws_app`）。
    Ask,
}

/// 总控 → APP。
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AppOut {
    /// 当前快照（`query` 的答复，或 `watch` 建立时先给的那一份）。`payload` 是密文，APP 自己解。
    Snapshot {
        instance_id: String,
        online: bool,
        payload: Option<String>,
    },
    /// 订阅期间载荷变了。
    Update {
        instance_id: String,
        payload: String,
    },
    /// 订阅期间设备断开。
    Offline { instance_id: String },
    /// 已把这句「请自检」交到设备那条连接上（设备还没回）。
    ///
    /// 单独回这一句，是为了让手机分得清「总控没收到」和「电脑没回话」—— 后者多半是插件
    /// 版本太旧（还不认识这条消息），说成手机网络问题会让人查错方向。
    Asked { instance_id: String },
    /// 设备回的自检结果（密文，APP 自己解）。
    Diagnosis { instance_id: String, payload: String },
    /// 这次自检没能转达：设备此刻不在总控上（插件没跑 / 电脑没出网 / 没开自动上报）。
    DiagnosisUnavailable { instance_id: String, reason: String },
    Error { message: String },
}
