use clap::Args;
use serde::{Deserialize, Serialize};

/// RPC 请求（一行 JSON 一个请求）
#[derive(Debug, Serialize, Deserialize)]
pub struct Req {
    pub v: u32,
    pub cmd: String,
    #[serde(default)]
    pub args: serde_json::Value,
}

/// RPC 响应
#[derive(Debug, Serialize, Deserialize)]
pub struct Resp {
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl Resp {
    pub fn ok(data: serde_json::Value) -> Self {
        Self {
            ok: true,
            data: Some(data),
            error: None,
        }
    }
    pub fn err(msg: impl Into<String>) -> Self {
        Self {
            ok: false,
            data: None,
            error: Some(msg.into()),
        }
    }
}

// ---- 命令参数结构（clap CLI 与 RPC 共用单一真源）----

#[derive(Args, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct SubUpdateParams {
    /// 仅更新指定订阅（缺省全部）
    #[arg(long)]
    pub name: Option<String>,
    /// 订阅清单 JSON 路径（CLI 解析后的绝对路径）
    #[arg(long)]
    pub subs: Option<String>,
}

#[derive(Args, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TestParams {
    #[arg(long, default_value = "hybrid")]
    pub mode: String, // tcping|realping|hybrid
    #[arg(long, default_value_t = 32)]
    pub concurrency: usize,
    #[arg(long, default_value_t = 5)]
    pub timeout: u64,
    #[arg(long, default_value_t = false)]
    pub with_ipinfo: bool,
    #[arg(long)]
    pub filter: Option<String>,
    #[arg(long, default_value_t = 1000)]
    pub top: usize,
}

impl Default for TestParams {
    fn default() -> Self {
        Self {
            mode: "hybrid".into(),
            concurrency: 32,
            timeout: 5,
            with_ipinfo: false,
            filter: None,
            top: 1000,
        }
    }
}

#[derive(Args, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct PruneParams {
    #[arg(long, default_value_t = 0)]
    pub delay_threshold: i32,
    #[arg(long)]
    pub keep_top: Option<usize>,
    #[arg(long, default_value_t = false)]
    pub dedup_endpoint: bool,
    /// 清理畸形节点（内网/保留地址、端口 0）
    #[arg(long, default_value_t = false)]
    pub invalid: bool,
}

#[derive(Args, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ProbeParams {
    #[arg(long)]
    pub batch_size: Option<usize>,
    #[arg(long)]
    pub timeout: Option<u64>,
    #[arg(long)]
    pub probe_url: Option<String>,
    #[arg(long)]
    pub max_batches: Option<usize>,
    #[arg(long)]
    pub concurrency: Option<usize>,
    #[arg(long)]
    pub filter: Option<String>,
}

impl Default for ProbeParams {
    fn default() -> Self {
        Self {
            batch_size: None,
            timeout: None,
            probe_url: None,
            max_batches: None,
            concurrency: None,
            filter: None,
        }
    }
}

/// 一键全流程参数（CLI 与 RPC 共用；follow 仅 CLI 侧）
#[derive(Args, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AutoParams {
    /// 仅更新指定订阅（缺省全部）
    #[arg(long)]
    pub name: Option<String>,
    /// 节点过滤正则（作用于 sub/协议/地址/凭证，影响 test/probe）
    #[arg(long)]
    pub filter: Option<String>,
    /// 代理端口
    #[arg(long, default_value_t = 10808)]
    pub port: u16,
    #[arg(long, default_value_t = 200)]
    pub test_concurrency: usize,
    #[arg(long, default_value_t = 3)]
    pub test_timeout: u64,
    /// 剪枝保留条数（缺省全保留存活节点）
    #[arg(long)]
    pub keep_top: Option<usize>,
    #[arg(long)]
    pub batch_size: Option<usize>,
    #[arg(long)]
    pub max_batches: Option<usize>,
    #[arg(long)]
    pub probe_concurrency: Option<usize>,
    #[arg(long)]
    pub probe_timeout: Option<u64>,
    #[arg(long)]
    pub probe_url: Option<String>,
    /// 失败自动顺延的最多尝试数
    #[arg(long, default_value_t = 3)]
    pub retries: usize,
    /// 代理前台运行（缺省 daemon）；auto 自身始终是 server 内后台 job
    #[arg(long, default_value_t = false)]
    pub no_daemon: bool,
    #[arg(long, default_value_t = false)]
    pub skip_update: bool,
    #[arg(long, default_value_t = false)]
    pub skip_test: bool,
    #[arg(long, default_value_t = false)]
    pub skip_prune: bool,
    #[arg(long, default_value_t = false)]
    pub skip_probe: bool,
    /// 订阅清单路径（CLI 解析后的绝对路径）
    #[arg(long)]
    pub subs: Option<String>,
}

impl Default for AutoParams {
    fn default() -> Self {
        Self {
            name: None,
            filter: None,
            port: 10808,
            test_concurrency: 200,
            test_timeout: 3,
            keep_top: None,
            batch_size: None,
            max_batches: None,
            probe_concurrency: None,
            probe_timeout: None,
            probe_url: None,
            retries: 3,
            no_daemon: false,
            skip_update: false,
            skip_test: false,
            skip_prune: false,
            skip_probe: false,
            subs: None,
        }
    }
}

#[derive(Args, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RunParams {
    #[arg(long, default_value_t = 10808)]
    pub port: u16,
    #[arg(long, default_value_t = 1)]
    pub count: usize,
    #[arg(long)]
    pub ports: Option<String>, // 逗号分隔
    #[arg(long, default_value_t = false)]
    pub distinct_cc: bool,
    /// 选节点策略：score=综合评分优先（默认，速度为主延迟折算）、least-latency=最低延迟、random=随机
    #[arg(long, default_value = "score")]
    pub strategy: String,
    #[arg(long, default_value_t = false)]
    pub daemon: bool,
    #[arg(long)]
    pub filter: Option<String>,
    /// 单端口 daemon 失败自动顺延的最多尝试数（多端口/前台不适用）
    #[arg(long, default_value_t = 3)]
    pub retries: usize,
    /// 启动后自验证的目标 URL（单端口 daemon）；缺省读 config.toml 的 probe_url
    #[arg(long)]
    pub verify_url: Option<String>,
}

impl Default for RunParams {
    fn default() -> Self {
        Self {
            port: 10808,
            count: 1,
            ports: None,
            distinct_cc: false,
            strategy: "score".into(),
            daemon: false,
            filter: None,
            retries: 3,
            verify_url: None,
        }
    }
}

#[derive(Args, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct StopParams {
    #[arg(long)]
    pub port: Option<u16>,
    #[arg(long, default_value_t = false)]
    pub all: bool,
}

#[derive(Args, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct SwitchParams {
    #[arg(long, default_value = "next")]
    pub which: String, // next/prev/random/index:3
    #[arg(long)]
    pub port: Option<u16>,
    #[arg(long, default_value_t = false)]
    pub all: bool,
    /// 仅更新映射，不重启 sing-box
    #[arg(long, default_value_t = false)]
    pub no_restart: bool,
}

#[derive(Args, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct IpinfoParams {
    #[arg(long, default_value_t = 16)]
    pub concurrency: usize,
}

/// 看护开关参数
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchArgs {
    pub port: u16,
    #[serde(default)]
    pub filter: Option<String>,
    #[serde(default)]
    pub probe_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchStopArgs {
    pub port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobIdArgs {
    pub job_id: u64,
}
