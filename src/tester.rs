use std::sync::PoisonError;
use anyhow::Result;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use std::{
    collections::{HashMap, HashSet},
    sync::{Mutex, OnceLock},
};
use tokio::net::TcpStream;

use crate::model::Node;

static PROXY_CLIENTS: OnceLock<Mutex<HashMap<(String, u64), reqwest::Client>>> = OnceLock::new();
static SINGBOX_BIN: OnceLock<Mutex<Option<PathBuf>>> = OnceLock::new();

/// 按 (代理, 超时) 缓存复用 Client：每次新建会丢掉连接池，几百节点就是几百次重复 TLS 握手
fn client_for(proxy_url: &str, timeout_secs: u64) -> Option<reqwest::Client> {
    let mut m = PROXY_CLIENTS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    let key = (proxy_url.to_string(), timeout_secs);
    if let Some(c) = m.get(&key) {
        return Some(c.clone());
    }
    let proxy = reqwest::Proxy::all(proxy_url).ok()?;
    let client = reqwest::Client::builder()
        .proxy(proxy)
        .timeout(Duration::from_secs(timeout_secs))
        .user_agent("proxytool/0.1")
        .build()
        .ok()?;
    m.insert(key, client.clone());
    drop(m);
    Some(client)
}

/// sing-box 路径缓存：成功才记；未安装时每次重查，装完即生效
pub fn singbox_bin() -> Option<PathBuf> {
    let cache = SINGBOX_BIN.get_or_init(|| Mutex::new(None));
    let value = cache.lock().unwrap_or_else(PoisonError::into_inner).clone();
    if let Some(p) = value {
        return Some(p);
    }
    let p = which::which("sing-box").ok()?;
    *cache.lock().unwrap_or_else(PoisonError::into_inner) = Some(p.clone());
    Some(p)
}

pub async fn tcping(addr: &str, port: u16, timeout: Duration) -> i32 {
    let target = format!("{addr}:{port}");
    let start = Instant::now();
    let res = tokio::time::timeout(timeout, TcpStream::connect(target)).await;
    match res {
        Ok(Ok(_)) => i32::try_from(start.elapsed().as_millis()).unwrap_or(i32::MAX),
        _ => -1,
    }
}

fn apply_tcping_result(node: &mut Node, delay_ms: i32) {
    node.delay_ms = if delay_ms >= 0 { delay_ms.max(1) } else { -1 };
    node.alive = delay_ms >= 0;
    node.speed_kbps = None;
    node.exit_ip = None;
    node.cc = None;
    node.probed = false;
    node.last_test_at = Some(chrono::Utc::now());
}

pub async fn test_nodes_tcping(nodes: &mut [Node], concurrency: usize, timeout_secs: u64) {
    let timeout = Duration::from_secs(timeout_secs);
    let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(concurrency.max(1)));
    let mut handles = Vec::new();
    let addrs: Vec<(String, u16)> = nodes.iter().map(|n| (n.addr.clone(), n.port)).collect();
    for (idx, (addr, port)) in addrs.into_iter().enumerate() {
        let sem = sem.clone();
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire_owned().await.unwrap();
            let d = tcping(&addr, port, timeout).await;
            (idx, d)
        }));
    }
    let total = handles.len();
    let mut done = 0usize;
    for h in handles {
        if let Ok((idx, d)) = h.await {
            apply_tcping_result(&mut nodes[idx], d);
            done += 1;
            if done.is_multiple_of(500) || done == total {
                tracing::info!("tcping 进度 {done}/{total}");
            }
        }
    }
}

// RealPing：通过临时 sing-box socks 代理测试
pub async fn test_nodes_realping(
    nodes: &mut [Node],
    concurrency: usize,
    timeout_secs: u64,
    with_ipinfo: bool,
    ip_api_url: &str,
    probe_url: &str,
) -> Result<()> {
    if singbox_bin().is_none() {
        return Err(anyhow::anyhow!(
            "realping 需要 sing-box；如只需端口筛选请使用 --mode tcping"
        ));
    }

    let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(concurrency.max(1)));
    let probe_url = probe_url.to_string();
    let ip_api_url = ip_api_url.to_string();
    let clones = nodes.to_vec();
    let mut handles = Vec::with_capacity(clones.len());
    for (idx, node) in clones.into_iter().enumerate() {
        let sem = sem.clone();
        let probe_url = probe_url.clone();
        let ip_api_url = ip_api_url.clone();
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire_owned().await.unwrap();
            let mut result = probe_single_node(&node, &probe_url, &ip_api_url, timeout_secs).await;
            if !with_ipinfo {
                result.ip = None;
                result.cc = None;
            }
            (idx, result)
        }));
    }
    for handle in handles {
        if let Ok((idx, result)) = handle.await
            && let Some(node) = nodes.get_mut(idx)
        {
            apply_probe_result(node, result);
        }
    }
    Ok(())
}

pub fn is_probe_success(status: u16) -> bool {
    (200..400).contains(&status)
}

/// 反滥用限流：连接、DNS、TLS、HTTP 全通，只是目标方挡了页面内容
pub const fn is_rate_limited(status: u16) -> bool {
    matches!(status, 429 | 403)
}

/// 节点是否把请求送到了目标站：2xx/3xx 成功，429/403 表示连接/DNS/TLS/HTTP 全通，
/// 只是目标方按出口 IP 拒绝了内容。**这是所有"该节点能不能用"判定的唯一真源**，
/// 探测/验证/切换/看护必须共用，否则会出现"probe 选出、run 又标死"的自相残杀
pub fn is_reachable(status: u16) -> bool {
    is_probe_success(status) || is_rate_limited(status)
}

/// 经代理的健康判定：所有"该节点能不能用"的路径（看护/切换/启动验证）共用
pub enum Health {
    /// 目标站正常响应
    Ok,
    /// 目标拒绝该出口（按出口 IP 挡），但出口本身能上网：节点在网，换节点也无解
    TargetRefused,
    /// 目标与出口都不通：节点假死
    Dead,
}

/// 先探目标，不通再探出口区分「节点假死」与「目标站拒绝该出口」
/// **只以出口是否可用判节点死活**：目标站按 IP 拒绝（如 chatgpt 挡机场出口）时若判死，
/// 会逐个标死候选，一轮轮扫下去能把整个节点池清空
pub async fn health_via_proxy(port: u16, target: &str, ip_url: &str, timeout: u64) -> Health {
    let proxy = socks_proxy_url(port);
    let hit = http_get_via_socks(&proxy, target, timeout, NO_BODY)
        .await
        .is_some_and(|(s, _, _)| is_reachable(s));
    if hit {
        return Health::Ok;
    }
    let exit_ok = http_get_via_socks(&proxy, ip_url, IPINFO_TIMEOUT_SECS, NO_BODY)
        .await
        .is_some_and(|(s, _, _)| is_reachable(s));
    if exit_ok {
        Health::TargetRefused
    } else {
        Health::Dead
    }
}

/// 速度 KB/s = 字节 / 1024 / 秒
pub fn calc_speed_kbps(bytes: usize, elapsed_ms: i32) -> Option<f64> {
    if elapsed_ms <= 0 {
        return None;
    }
    #[allow(clippy::cast_precision_loss, reason = "速度计算只需近似精度，f64 足够")]
    Some(bytes as f64 / 1024.0 / (f64::from(elapsed_ms) / 1000.0))
}

/// 计算分批区间 [start,end)，供小批量探测使用
pub fn calc_batches(
    total: usize,
    batch_size: usize,
    max_batches: Option<usize>,
) -> Vec<(usize, usize)> {
    if batch_size == 0 || total == 0 {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut start = 0;
    while start < total {
        if let Some(m) = max_batches
            && out.len() >= m
        {
            break;
        }
        let end = (start + batch_size).min(total);
        out.push((start, end));
        start = end;
    }
    out
}

pub fn socks_proxy_url(port: u16) -> String {
    format!("socks5h://127.0.0.1:{port}")
}

static RESERVED_PORTS: OnceLock<Mutex<HashSet<u16>>> = OnceLock::new();

struct ReservedPort(u16);

impl Drop for ReservedPort {
    fn drop(&mut self) {
        if let Some(ports) = RESERVED_PORTS.get() {
            let mut ports = ports
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            ports.remove(&self.0);
        }
    }
}

fn pick_free_port() -> Option<ReservedPort> {
    let ports = RESERVED_PORTS.get_or_init(|| Mutex::new(HashSet::new()));
    for _ in 0..32 {
        // 单次 bind 失败只重试，不直接返回（否则循环形同虚设）
        let Ok(listener) = std::net::TcpListener::bind("127.0.0.1:0") else {
            continue;
        };
        let Ok(addr) = listener.local_addr() else {
            continue;
        };
        let port = addr.port();
        drop(listener);
        let mut reserved = ports
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if reserved.insert(port) {
            return Some(ReservedPort(port));
        }
    }
    None
}

/// 验证/保活只要状态码，不读响应体
pub const NO_BODY: usize = 0;
/// 测速采样上限：读够即停。够算 KB/s，又不会被大页面拖到超时
/// （chatgpt.com 首版单次就 558KB，读全文会让慢节点误判成"无响应"）
pub const SPEED_SAMPLE_BYTES: usize = 256 * 1024;
/// 出口 IP 查询超时上限：只是附加信息，不该拖慢主流程
pub const IPINFO_TIMEOUT_SECS: u64 = 8;

/// 经由本地 socks 代理 GET 目标 URL，返回 (`http_status`, 已读字节数, 延迟ms)
/// `body_limit`：0 表示不读响应体；>0 表示最多读这么多字节即停
pub async fn http_get_via_socks(
    proxy_url: &str,
    target_url: &str,
    timeout_secs: u64,
    body_limit: usize,
) -> Option<(u16, usize, i32)> {
    let client = client_for(proxy_url, timeout_secs)?;
    let start = Instant::now();
    let mut resp = client.get(target_url).send().await.ok()?;
    let status = resp.status().as_u16();
    if body_limit == 0 {
        let elapsed = i32::try_from(start.elapsed().as_millis()).unwrap_or(i32::MAX);
        return Some((status, 0, elapsed));
    }
    let mut read = 0usize;
    while read < body_limit {
        match resp.chunk().await {
            Ok(Some(chunk)) => read += chunk.len(),
            Ok(None) => break,
            Err(_) => return None,
        }
    }
    let elapsed = i32::try_from(start.elapsed().as_millis()).unwrap_or(i32::MAX);
    Some((status, read, elapsed))
}

#[derive(Debug, Clone)]
pub struct ProbeResult {
    pub alive: bool,
    pub delay_ms: i32,
    pub speed_kbps: Option<f64>,
    pub ip: Option<String>,
    pub cc: Option<String>,
}

pub fn apply_probe_result(node: &mut Node, result: ProbeResult) {
    node.alive = result.alive;
    node.delay_ms = result.delay_ms;
    node.speed_kbps = result.speed_kbps;
    node.exit_ip = result.ip;
    node.cc = result.cc;
    node.last_test_at = Some(chrono::Utc::now());
    node.probed = result.alive;
}

/// 对单个节点启动临时 sing-box 实例，抓取 `probe_url`
/// 成功条件：代理端口就绪 + 经代理 GET 返回 2xx/3xx；速度=大小/耗时
#[allow(clippy::too_many_lines, reason = "探测流程包含启动/请求/清理等步骤，拆分反而增加状态传递成本")]
pub async fn probe_single_node(
    node: &Node,
    probe_url: &str,
    ip_api_url: &str,
    timeout_secs: u64,
) -> ProbeResult {
    let fail = ProbeResult {
        alive: false,
        delay_ms: -1,
        speed_kbps: None,
        ip: None,
        cc: None,
    };
    let Some(bin) = singbox_bin() else {
        tracing::warn!("未找到 sing-box，无法真实探测 {}", node.addr);
        return fail;
    };
    let Some(reserved_port) = pick_free_port() else {
        return fail;
    };
    let port = reserved_port.0;
    // 探测用临时实例只起在本机回环，不对外暴露
    // include 传空：探测必须全流量强制走节点，不受 network policy 影响
    let Ok(cfg) = crate::config_gen::generate_singbox_config(&[(node, port)], "127.0.0.1", &[]) else {
        return fail;
    };
    let cfg_path = crate::run::generate_config_path();
    let log_path = cfg_path.with_extension("log");
    let Ok(config_text) = serde_json::to_string_pretty(&cfg) else {
        return fail;
    };
    if std::fs::write(&cfg_path, config_text).is_err() {
        return fail;
    }
    let Ok(log_file) = std::fs::File::create(&log_path) else {
        let _ = std::fs::remove_file(&cfg_path);
        return fail;
    };
    let Ok(log_err) = log_file.try_clone() else {
        let _ = std::fs::remove_file(&cfg_path);
        let _ = std::fs::remove_file(&log_path);
        return fail;
    };
    let Ok(mut child) = tokio::process::Command::new(&bin)
        .args(["run", "-c", &cfg_path.to_string_lossy()])
        .stdout(std::process::Stdio::from(log_file))
        .stderr(std::process::Stdio::from(log_err))
        .spawn() else {
        let _ = std::fs::remove_file(&cfg_path);
        let _ = std::fs::remove_file(&log_path);
        return fail;
    };
    if !crate::run::wait_for_ports(&[port], 5000).await {
        let _ = child.kill().await;
        let _ = child.wait().await;
        let _ = std::fs::remove_file(&cfg_path);
        let _ = std::fs::remove_file(&log_path);
        return fail;
    }
    let proxy_url = socks_proxy_url(port);
    // 先抓 probe_url，顺带算速度=大小/耗时；
    // 429/403 是反滥用限流：连接/DNS/TLS 全通，视为首页可用（机房共享出口高发，
    // 否则整批真可用节点会被误判保活型删除）；失败先重试一次防单次抖动，
    // 仍失败才回退 generate_204 保活（速度记空）
    let keepalive_url = url::Url::parse(probe_url).ok().and_then(|u| {
        let host = u.host_str()?;
        Some(format!("{}//{}/generate_204", u.scheme(), host))
    });
    let mut probe = http_get_via_socks(&proxy_url, probe_url, timeout_secs, SPEED_SAMPLE_BYTES).await;
    if let Some((status, _, _)) = &probe
        && !is_reachable(*status)
    {
        probe =
            http_get_via_socks(&proxy_url, probe_url, timeout_secs, SPEED_SAMPLE_BYTES).await;
    } else if probe.is_none() {
        probe =
            http_get_via_socks(&proxy_url, probe_url, timeout_secs, SPEED_SAMPLE_BYTES).await;
    }
    let mut result = fail.clone();
    let homepage_ok = if let Some((status, bytes_len, latency)) = probe
        && is_reachable(status)
    {
        result.alive = true;
        result.delay_ms = latency.max(1);
        result.speed_kbps = calc_speed_kbps(bytes_len, latency.max(1));
        true
    } else {
        false
    };
    if !homepage_ok
        && let Some(ref ka) = keepalive_url
        && let Some((status, latency)) = http_get_via_socks(
            &proxy_url,
            ka,
            timeout_secs.min(8),
            NO_BODY,
        )
        .await
        .map(|(s, _, l)| (s, l))
        && is_reachable(status)
        && !result.alive
    {
        result.alive = true;
        result.delay_ms = latency.max(1);
    }
    if result.alive
        && let Some((ip, cc)) =
            crate::ipinfo::fetch_ip_via_proxy(&proxy_url, ip_api_url, IPINFO_TIMEOUT_SECS).await
    {
        result.ip = Some(ip);
        result.cc = Some(cc);
    }
    let _ = child.kill().await;
    let _ = child.wait().await;
    let _ = std::fs::remove_file(&cfg_path);
    let _ = std::fs::remove_file(&log_path);
    tokio::time::sleep(Duration::from_millis(100)).await;
    result
}

/// 全局最优：首页可用节点中综合评分优先、延迟其次（跨批次比较用）
pub fn pick_best_homepage(nodes: &[Node]) -> Option<&Node> {
    let mut best: Option<&Node> = None;
    for n in nodes.iter().filter(|n| n.is_homepage_ok()) {
        match best {
            Some(b) => {
                let bs = crate::select::node_score(b);
                let s = crate::select::node_score(n);
                if s > bs || ((s - bs).abs() < f64::EPSILON && n.delay_ms < b.delay_ms) {
                    best = Some(n);
                }
            }
            None => best = Some(n),
        }
    }
    best
}

/// 并发探测一个批次，就地更新 nodes（选优统一由 `select::node_score` 在上层判定）
pub async fn probe_batch(
    batch: &mut [Node],
    probe_url: &str,
    ip_api_url: &str,
    timeout_secs: u64,
    concurrency: usize,
) {
    let concurrency = concurrency.max(1).min(batch.len().max(1));
    let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(concurrency));
    let clones: Vec<Node> = batch.to_vec();
    let probe_url = probe_url.to_string();
    let ip_api_url = ip_api_url.to_string();
    let mut handles = Vec::new();
    for (idx, node) in clones.into_iter().enumerate() {
        let sem = sem.clone();
        let probe_url = probe_url.clone();
        let ip_api_url = ip_api_url.clone();
        handles.push(tokio::spawn(async move {
            let _p = sem.acquire_owned().await.unwrap();
            let r = probe_single_node(&node, &probe_url, &ip_api_url, timeout_secs).await;
            (idx, r)
        }));
    }
    for h in handles {
        if let Ok((idx, r)) = h.await
            && let Some(n) = batch.get_mut(idx)
        {
            apply_probe_result(n, r);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calc_batches_basic() {
        assert_eq!(
            calc_batches(55, 20, None),
            vec![(0, 20), (20, 40), (40, 55)]
        );
    }

    #[test]
    fn test_calc_batches_max() {
        assert_eq!(calc_batches(100, 20, Some(2)), vec![(0, 20), (20, 40)]);
    }

    #[test]
    fn test_calc_batches_edge() {
        assert!(calc_batches(0, 20, None).is_empty());
        assert!(calc_batches(10, 0, None).is_empty());
        assert_eq!(calc_batches(5, 20, None), vec![(0, 5)]);
    }

    #[test]
    fn test_is_probe_success() {
        assert!(is_probe_success(204));
        assert!(is_probe_success(200));
        assert!(is_probe_success(301));
        assert!(!is_probe_success(404));
        assert!(!is_probe_success(500));
    }

    #[test]
    fn test_is_reachable_unifies_success_and_rate_limit() {
        assert!(is_reachable(200));
        assert!(is_reachable(301));
        assert!(is_reachable(399));
        // 429/403：目标方按出口 IP 拒绝内容，链路是全通的，必须算可用，
        // 否则会把 probe 选中的节点在验证环节标死（历史上真实发生过）
        assert!(is_reachable(403));
        assert!(is_reachable(429));
        assert!(!is_reachable(400));
        assert!(!is_reachable(404));
        assert!(!is_reachable(500));
    }

    #[test]
    fn test_is_rate_limited() {
        assert!(is_rate_limited(429));
        assert!(is_rate_limited(403));
        assert!(!is_rate_limited(200));
        assert!(!is_rate_limited(404));
        assert!(!is_rate_limited(500));
    }

    #[test]
    fn test_calc_speed_kbps() {
        let s = calc_speed_kbps(1024 * 100, 1000).unwrap();
        assert!((s - 100.0).abs() < 1e-6);
        assert!(calc_speed_kbps(100, 0).is_none());
    }

    #[test]
    fn test_apply_probe_failure_clears_stale_observations() {
        let mut node = Node::new(
            "s",
            crate::model::NodeType::Vless,
            "1.1.1.1",
            443,
            "vless://u@1.1.1.1:443",
        );
        node.alive = true;
        node.delay_ms = 100;
        node.speed_kbps = Some(42.0);
        node.exit_ip = Some("9.9.9.9".into());
        node.cc = Some("US".into());
        node.probed = true;
        apply_probe_result(
            &mut node,
            ProbeResult {
                alive: false,
                delay_ms: -1,
                speed_kbps: None,
                ip: None,
                cc: None,
            },
        );
        assert!(!node.alive);
        assert_eq!(node.delay_ms, -1);
        assert!(node.speed_kbps.is_none());
        assert!(node.exit_ip.is_none());
        assert!(node.cc.is_none());
        assert!(!node.probed);
    }

    #[test]
    fn test_apply_tcping_result_clears_probe_observations_and_accepts_zero_ms() {
        let mut node = Node::new(
            "s",
            crate::model::NodeType::Vless,
            "1.1.1.1",
            443,
            "vless://u@1.1.1.1:443",
        );
        node.alive = true;
        node.speed_kbps = Some(42.0);
        node.exit_ip = Some("9.9.9.9".into());
        node.cc = Some("US".into());
        node.probed = true;
        apply_tcping_result(&mut node, 0);
        assert!(node.alive);
        assert_eq!(node.delay_ms, 1);
        assert!(node.speed_kbps.is_none());
        assert!(node.exit_ip.is_none());
        assert!(node.cc.is_none());
        assert!(!node.probed);
    }

    fn homepage_node(id: &str, speed: Option<f64>, delay: i32) -> Node {
        let mut n = Node::new(
            "s",
            crate::model::NodeType::Vless,
            "1.1.1.1",
            443,
            &format!("vless://u@1.1.1.1:443#{id}"),
        );
        n.id = id.to_string();
        n.alive = speed.is_some() || delay > 0;
        // 首页可用 = 存活 + 有速度；保活型 = 存活但无速度
        if speed.is_some() {
            n.alive = true;
        }
        n.delay_ms = delay;
        n.speed_kbps = speed;
        n.probed = true;
        n
    }

    #[test]
    fn test_pick_best_homepage_prefers_speed_over_first() {
        let nodes = vec![
            homepage_node("a", Some(100.0), 100),
            homepage_node("b", Some(500.0), 300),
            homepage_node("c", Some(200.0), 50),
        ];
        let best = pick_best_homepage(&nodes).unwrap();
        assert_eq!(best.id, "b");
    }

    #[test]
    fn test_pick_best_homepage_penalizes_high_latency() {
        // 速度略低但延迟低一个量级者优先（故障实测数据的抽象）
        let nodes = vec![
            homepage_node("fast_slow_link", Some(37.4), 2245),
            homepage_node("balanced", Some(30.0), 400),
        ];
        let best = pick_best_homepage(&nodes).unwrap();
        assert_eq!(best.id, "balanced");
    }

    #[test]
    fn test_pick_best_homepage_tie_breaks_by_delay() {
        let nodes = vec![
            homepage_node("a", Some(200.0), 300),
            homepage_node("b", Some(200.0), 100),
        ];
        let best = pick_best_homepage(&nodes).unwrap();
        assert_eq!(best.id, "b");
    }

    #[test]
    fn test_pick_best_homepage_ignores_non_homepage() {
        let mut fallback = homepage_node("fallback", None, 50);
        fallback.alive = true;
        let dead = {
            let mut n = homepage_node("dead", None, -1);
            n.alive = false;
            n
        };
        let ok = homepage_node("ok", Some(10.0), 500);
        let nodes = vec![fallback, dead, ok];
        let best = pick_best_homepage(&nodes).unwrap();
        assert_eq!(best.id, "ok");
    }

    #[test]
    fn test_pick_best_homepage_none_when_no_homepage_ok() {
        let mut fallback = homepage_node("fallback", None, 50);
        fallback.alive = true;
        assert!(pick_best_homepage(&[fallback]).is_none());
        assert!(pick_best_homepage(&[]).is_none());
    }
}
