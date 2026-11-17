//! 线上协议的数据形状（设备↔总控、APP↔总控）。
//!
//! 一条地址只描述「用什么协议连哪里、是哪条通道」：`kind` 是通道类型
//! （`relay` / `lan` / `ipv6` / `direct`），`url` 是完整地址。设备把手上所有可用地址
//! 一起报上来，APP 拿到后自己挑。

use serde::{Deserialize, Serialize};

/// 设备的一条连接地址。`kind` 缺省时按「未分类」处理，APP 侧不应依赖它一定存在。
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Address {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    pub url: String,
}

/// 设备 → 总控。
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DeviceIn {
    /// 首次上线 / 重连后上报整组地址。
    Register { addresses: Vec<Address> },
    /// 地址变化时上报新的整组地址（不是增量）。
    Update { addresses: Vec<Address> },
}

/// 总控 → 设备。
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DeviceOut {
    /// 注册成功，带回这台设备的令牌。插件留着它，交给 APP。
    Registered { instance_id: String, token: String },
    /// 一次地址更新已被收下（没有变化时也回，方便设备确认送达）。
    Updated,
    Error { message: String },
}

/// APP → 总控。
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AppIn {
    /// 查一次当前地址（一次性问答）。
    Query,
    /// 订阅这台设备：先回一份当前快照，之后只在有变化时推送。
    Watch,
    /// 取消订阅。
    Unwatch,
}

/// 总控 → APP。
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AppOut {
    /// 当前快照（`query` 的答复，或 `watch` 建立时先给的那一份）。
    Snapshot {
        instance_id: String,
        online: bool,
        addresses: Vec<Address>,
    },
    /// 订阅期间地址变了。
    Update {
        instance_id: String,
        addresses: Vec<Address>,
    },
    /// 订阅期间设备断开。
    Offline { instance_id: String },
    Error { message: String },
}

/// 清洗一组地址：去空白、丢掉空地址、按 (kind, url) 去重，保留首次出现的顺序。
pub fn normalize_addresses(list: Vec<Address>) -> Vec<Address> {
    let mut out: Vec<Address> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for item in list {
        let Address { kind, url } = item;
        let url = url.trim().to_string();
        if url.is_empty() {
            continue;
        }
        let kind = kind
            .map(|k| k.trim().to_string())
            .filter(|k| !k.is_empty());
        let key = (kind.clone().unwrap_or_default(), url.clone());
        if seen.insert(key) {
            out.push(Address { kind, url });
        }
    }
    out
}

/// 用于比较「地址有没有变」的规范形态：与顺序无关、只由 (kind, url) 决定。
/// 设备重连时把地址换个顺序不该被当成一次变化去惊动订阅者。
pub fn canonical(list: &[Address]) -> Vec<(String, String)> {
    let mut v: Vec<(String, String)> = list
        .iter()
        .map(|a| (a.kind.clone().unwrap_or_default(), a.url.clone()))
        .collect();
    v.sort();
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(kind: Option<&str>, url: &str) -> Address {
        Address {
            kind: kind.map(|k| k.to_string()),
            url: url.to_string(),
        }
    }

    #[test]
    fn normalize_drops_empty_and_dupes_keeps_order() {
        let input = vec![
            addr(Some("relay"), "wss://a"),
            addr(Some("relay"), "  wss://a  "),
            addr(None, ""),
            addr(Some("lan"), "http://192.168.1.2:1"),
        ];
        let out = normalize_addresses(input);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].url, "wss://a");
        assert_eq!(out[1].url, "http://192.168.1.2:1");
    }

    #[test]
    fn canonical_ignores_order() {
        let a = vec![addr(Some("relay"), "wss://a"), addr(Some("lan"), "http://b")];
        let b = vec![addr(Some("lan"), "http://b"), addr(Some("relay"), "wss://a")];
        assert_eq!(canonical(&a), canonical(&b));
    }

    #[test]
    fn canonical_detects_content_change() {
        let a = vec![addr(Some("relay"), "wss://a")];
        let b = vec![addr(Some("relay"), "wss://c")];
        assert_ne!(canonical(&a), canonical(&b));
    }
}
