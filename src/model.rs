use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum NodeType {
    Vmess,
    Vless,
    Trojan,
    Shadowsocks,
    Socks,
    Hysteria2, // hy2 / hysteria2
    Tuic,
    Wireguard,
    AnyTls,
    Naive,
    Http,
    Unknown(String),
}

impl NodeType {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Vmess => "vmess",
            Self::Vless => "vless",
            Self::Trojan => "trojan",
            Self::Shadowsocks => "ss",
            Self::Socks => "socks",
            Self::Hysteria2 => "hysteria2",
            Self::Tuic => "tuic",
            Self::Wireguard => "wireguard",
            Self::AnyTls => "anytls",
            Self::Naive => "naive",
            Self::Http => "http",
            Self::Unknown(s) => s.as_str(),
        }
    }
    pub fn from_scheme(s: &str) -> Self {
        match s.to_ascii_lowercase().as_str() {
            "vmess" => Self::Vmess,
            "vless" => Self::Vless,
            "trojan" => Self::Trojan,
            "ss" | "shadowsocks" => Self::Shadowsocks,
            "socks" | "socks5" | "socks5h" | "socks4" | "socks4a" => Self::Socks,
            "hysteria2" | "hy2" | "hy" => Self::Hysteria2,
            "tuic" => Self::Tuic,
            "wireguard" => Self::Wireguard,
            "anytls" => Self::AnyTls,
            "naive" | "naive+https" | "naive+quic" => Self::Naive,
            "http" | "https" => Self::Http,
            other => Self::Unknown(other.to_string()),
        }
    }
}

fn default_delay() -> i32 {
    -1
}

/// 精简后节点：[订阅名/协议/ip/port/凭证] + [出口ip/延迟/速度/存活]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub id: String,
    #[serde(default)]
    pub sub: String,
    pub r#type: NodeType,
    pub addr: String,
    pub port: u16,
    #[serde(alias = "uri")]
    pub cred: String,
    #[serde(default = "default_delay")]
    pub delay_ms: i32, // -1 未测 / >0 延迟ms
    #[serde(default)]
    pub alive: bool,
    #[serde(default, alias = "ip")]
    pub exit_ip: Option<String>,
    #[serde(default)]
    pub cc: Option<String>,
    #[serde(default)]
    pub speed_kbps: Option<f64>,
    pub last_test_at: Option<DateTime<Utc>>,
    /// 是否经过 probe 真实探测（tcping 不算）；用于区分保活型误删 tcping 池
    #[serde(default)]
    pub probed: bool,
}

impl Node {
    pub fn new(sub: &str, t: NodeType, addr: &str, port: u16, cred: &str) -> Self {
        let id = format!("{:x}", md5::compute(cred));
        Self {
            id,
            sub: sub.to_string(),
            r#type: t,
            addr: addr.to_string(),
            port,
            cred: cred.to_string(),
            delay_ms: -1,
            alive: false,
            exit_ip: None,
            cc: None,
            speed_kbps: None,
            last_test_at: None,
            probed: false,
        }
    }

    /// 首页探测成功：存活且抓到页面算出速度
    pub fn is_homepage_ok(&self) -> bool {
        self.alive && self.speed_kbps.is_some()
    }

    /// 保活型：probe 测过、标存活，但无速度（仅 generate_204 通过）
    /// tcping 筛过（probed=false）与未测节点不算在内，不得误删
    pub fn is_fallback_only(&self) -> bool {
        self.alive && self.probed && self.speed_kbps.is_none()
    }
}

/// subs.json 单项：name 缺省用序号补齐
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubConfig {
    pub name: Option<String>,
    pub url: String,
}

/// state 内仅保留更新时间（URL 以 subs.json 为准）
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SubMeta {
    pub name: String,
    pub updated_at: Option<DateTime<Utc>>,
}

/// 运行中代理（daemon 落盘，用于 status/stop）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunningProxy {
    pub port: u16,
    pub node_id: String,
    pub pid: u32,
    pub config_path: String,
    pub log_path: String,
    pub started_at: Option<DateTime<Utc>>,
}

/// 解析 subs.json 后的 (name, url)，空名按 1-based 序号补齐
pub fn resolve_sub_names(cfgs: &[SubConfig]) -> Vec<(String, String)> {
    cfgs.iter()
        .enumerate()
        .map(|(i, c)| {
            let name = c.name.clone().unwrap_or_default();
            let name = name.trim().to_string();
            let name = if name.is_empty() {
                (i + 1).to_string()
            } else {
                name
            };
            (name, c.url.clone())
        })
        .collect()
}

/// 运行态唯一真源是 `running`（store 的 running 表），其余展示/切换所需信息均由它派生
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AppState {
    #[serde(default)]
    pub subs: Vec<SubMeta>,
    #[serde(default)]
    pub nodes: Vec<Node>,
    #[serde(default)]
    pub running: Vec<RunningProxy>,
    #[serde(default)]
    pub settings: Settings,
    /// server 附加状态（server.pid、watch:<port> 等，B 架构 server 唯一写者）
    #[serde(default)]
    pub meta: BTreeMap<String, String>,
}

/// 看护配置（server 内常驻任务；serve 重启后按 meta 恢复）
/// last_ok/last_check/fail_count 由看护循环每周期写回，供 status 展示
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WatchConfig {
    #[serde(default)]
    pub filter: Option<String>,
    #[serde(default)]
    pub verify_url: String,
    #[serde(default)]
    pub last_ok: Option<bool>,
    #[serde(default)]
    pub last_check: Option<DateTime<Utc>>,
    #[serde(default)]
    pub fail_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Settings {
    pub test_concurrency: usize,
    pub timeout_secs: u64,
    pub page_size: usize,
    pub ip_api_url: String,
    pub speed_ping_url: String,
    #[serde(default = "default_watch_interval")]
    pub watch_interval_secs: u64,
    #[serde(default = "default_watch_timeout")]
    pub watch_timeout_secs: u64,
    #[serde(default = "default_watch_threshold")]
    pub watch_fail_threshold: usize,
    #[serde(default = "default_watch_cooldown")]
    pub watch_cooldown_secs: u64,
    #[serde(default = "default_replace_ratio")]
    pub replace_speed_ratio: f64,
}

fn default_watch_interval() -> u64 {
    30
}

fn default_watch_timeout() -> u64 {
    10
}

fn default_watch_threshold() -> usize {
    2
}

fn default_watch_cooldown() -> u64 {
    60
}

fn default_replace_ratio() -> f64 {
    1.10
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            test_concurrency: 32,
            timeout_secs: 5,
            page_size: 1000,
            ip_api_url: "https://api.ip.sb/geoip".to_string(),
            speed_ping_url: "https://chatgpt.com".to_string(),
            watch_interval_secs: default_watch_interval(),
            watch_timeout_secs: default_watch_timeout(),
            watch_fail_threshold: default_watch_threshold(),
            watch_cooldown_secs: default_watch_cooldown(),
            replace_speed_ratio: default_replace_ratio(),
        }
    }
}

#[cfg(test)]
mod running_tests {
    use super::*;
    #[test]
    fn test_running_proxy_roundtrip() {
        let r = RunningProxy {
            port: 18282,
            node_id: "abc".into(),
            pid: 12345,
            config_path: "/tmp/x.json".into(),
            log_path: "/tmp/x.log".into(),
            started_at: None,
        };
        let s = serde_json::to_string(&r).unwrap();
        let back: RunningProxy = serde_json::from_str(&s).unwrap();
        assert_eq!(back.pid, 12345);
        assert_eq!(back.port, 18282);
    }
}

#[cfg(test)]
mod model_new_tests {
    use super::*;

    #[test]
    fn test_fallback_only_classification() {
        // 首页成功：有速度 => 首页可用，不是保活型
        let mut homepage = Node::new(
            "s",
            NodeType::Vless,
            "1.1.1.1",
            443,
            "vless://u@1.1.1.1:443#h",
        );
        homepage.alive = true;
        homepage.delay_ms = 200;
        homepage.probed = true;
        homepage.speed_kbps = Some(50.0);
        homepage.exit_ip = Some("9.9.9.9".into());
        assert!(homepage.is_homepage_ok());
        assert!(!homepage.is_fallback_only());
        // 仅保活：probe 测过、存活、无速度 => 保活型（即使抓到了出口 IP 也算）
        let mut fallback = Node::new(
            "s",
            NodeType::Vless,
            "2.2.2.2",
            443,
            "vless://u@2.2.2.2:443#f",
        );
        fallback.alive = true;
        fallback.delay_ms = 2069;
        fallback.probed = true;
        assert!(!fallback.is_homepage_ok());
        assert!(fallback.is_fallback_only());
        // tcping 筛过但没 probe 过：不得误删
        let mut screened = Node::new(
            "s",
            NodeType::Vless,
            "3.3.3.3",
            443,
            "vless://u@3.3.3.3:443#t",
        );
        screened.alive = true;
        screened.delay_ms = 100;
        assert!(!screened.is_fallback_only());
        // 未测节点：不算保活型
        let fresh = Node::new(
            "s",
            NodeType::Vless,
            "4.4.4.4",
            443,
            "vless://u@4.4.4.4:443#n",
        );
        assert!(!fresh.is_fallback_only());
        // 测过但已死：不算保活型
        let mut dead = Node::new(
            "s",
            NodeType::Vless,
            "5.5.5.5",
            443,
            "vless://u@5.5.5.5:443#d",
        );
        dead.alive = false;
        dead.delay_ms = -1;
        dead.probed = true;
        assert!(!dead.is_fallback_only());
    }

    #[test]
    fn test_node_new_minimal() {
        let n = Node::new(
            "1",
            NodeType::Vless,
            "1.1.1.1",
            443,
            "vless://uuid@1.1.1.1:443#x",
        );
        assert_eq!(n.sub, "1");
        assert_eq!(n.addr, "1.1.1.1");
        assert_eq!(n.port, 443);
        assert_eq!(n.cred, "vless://uuid@1.1.1.1:443#x");
        assert_eq!(n.delay_ms, -1);
        assert!(!n.alive);
    }

    #[test]
    fn test_old_json_migrates() {
        let old = r#"{"id":"abc","uri":"vless://u@1.1.1.1:443#t","type":"vless","remarks":"别名","addr":"1.1.1.1","port":443,"raw_query":"a=1","delay_ms":100,"alive":true,"ip":"9.9.9.9","cc":"US","sort":10,"fail_count":0}"#;
        let n: Node = serde_json::from_str(old).unwrap();
        assert_eq!(n.cred, "vless://u@1.1.1.1:443#t");
        assert_eq!(n.exit_ip.as_deref(), Some("9.9.9.9"));
        assert_eq!(n.delay_ms, 100);
    }

    #[test]
    fn test_resolve_sub_names_fallback() {
        let cfgs = vec![
            SubConfig {
                name: None,
                url: "http://a".into(),
            },
            SubConfig {
                name: Some("".into()),
                url: "http://b".into(),
            },
            SubConfig {
                name: Some("my".into()),
                url: "http://c".into(),
            },
        ];
        let resolved = resolve_sub_names(&cfgs);
        assert_eq!(resolved[0].0, "1");
        assert_eq!(resolved[1].0, "2");
        assert_eq!(resolved[2].0, "my");
    }

    #[test]
    fn test_watch_config_old_json_compat() {
        // 旧 meta（无运行态字段）可解析；新字段走 Default
        let w: WatchConfig = serde_json::from_str(r#"{"verify_url":"x","filter":"HK"}"#).unwrap();
        assert_eq!(w.verify_url, "x");
        assert_eq!(w.filter.as_deref(), Some("HK"));
        assert!(w.last_ok.is_none() && w.fail_count == 0);
        // 新字段 roundtrip
        let w2: WatchConfig = serde_json::from_str(
            r#"{"verify_url":"x","last_ok":false,"last_check":"2026-01-01T00:00:00Z","fail_count":2}"#,
        )
        .unwrap();
        assert_eq!(w2.last_ok, Some(false));
        assert_eq!(w2.fail_count, 2);
    }
}
