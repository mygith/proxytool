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
        .unwrap_or_else(|p| p.into_inner());
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
    Some(client)
}

/// sing-box 路径缓存：成功才记；未安装时每次重查，装完即生效
pub(crate) fn singbox_bin() -> Option<PathBuf> {
    let cache = SINGBOX_BIN.get_or_init(|| Mutex::new(None));
    if let Some(p) = cache.lock().unwrap_or_else(|p| p.into_inner()).clone() {
        return Some(p);
    }
    let p = which::which("sing-box").ok()?;
    *cache.lock().unwrap_or_else(|p| p.into_inner()) = Some(p.clone());
    Some(p)
}

pub async fn tcping(addr: &str, port: u16, timeout: Duration) -> i32 {
    let target = format!("{addr}:{port}");
    let start = Instant::now();
    let res = tokio::time::timeout(timeout, TcpStream::connect(target)).await;
    match res {
        Ok(Ok(_)) => start.elapsed().as_millis() as i32,
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

/// 速度 KB/s = 字节 / 1024 / 秒
pub fn calc_speed_kbps(bytes: usize, elapsed_ms: i32) -> Option<f64> {
    if elapsed_ms <= 0 {
        return None;
    }
    Some(bytes as f64 / 1024.0 / (elapsed_ms as f64 / 1000.0))
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
                .unwrap_or_else(|poisoned| poisoned.into_inner());
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
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if reserved.insert(port) {
            return Some(ReservedPort(port));
        }
    }
    None
}

/// 经由本地 socks 代理 GET 目标 URL，返回 (http_status, 字节数, 延迟ms)
pub async fn http_get_via_socks(
    proxy_url: &str,
    target_url: &str,
    timeout_secs: u64,
) -> Option<(u16, usize, i32)> {
    let client = client_for(proxy_url, timeout_secs)?;
    let start = Instant::now();
    let resp = client.get(target_url).send().await.ok()?;
    let status = resp.status().as_u16();
    let bytes = resp.bytes().await.ok()?;
    let elapsed = start.elapsed().as_millis() as i32;
    Some((status, bytes.len(), elapsed))
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

/// 对单个节点启动临时 sing-box 实例，抓取 probe_url（默认 www.google.com 首页）
/// 成功条件：代理端口就绪 + 经代理 GET 返回 2xx/3xx；速度=大小/耗时
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
    let bin = match singbox_bin() {
        Some(p) => p,
        None => {
            tracing::warn!("未找到 sing-box，无法真实探测 {}", node.addr);
            return fail;
        }
    };
    let reserved_port = match pick_free_port() {
        Some(p) => p,
        None => return fail,
    };
    let port = reserved_port.0;
    // 探测用临时实例只起在本机回环，不对外暴露
    let cfg = match crate::config_gen::generate_singbox_config(&[(node, port)], "127.0.0.1") {
        Ok(c) => c,
        Err(_) => return fail,
    };
    let cfg_path = crate::run::generate_config_path();
    let log_path = cfg_path.with_extension("log");
    let config_text = match serde_json::to_string_pretty(&cfg) {
        Ok(text) => text,
        Err(_) => return fail,
    };
    if std::fs::write(&cfg_path, config_text).is_err() {
        return fail;
    }
    let log_file = match std::fs::File::create(&log_path) {
        Ok(file) => file,
        Err(_) => {
            let _ = std::fs::remove_file(&cfg_path);
            return fail;
        }
    };
    let log_err = match log_file.try_clone() {
        Ok(file) => file,
        Err(_) => {
            let _ = std::fs::remove_file(&cfg_path);
            let _ = std::fs::remove_file(&log_path);
            return fail;
        }
    };
    let mut child = match tokio::process::Command::new(&bin)
        .args(["run", "-c", &cfg_path.to_string_lossy()])
        .stdout(std::process::Stdio::from(log_file))
        .stderr(std::process::Stdio::from(log_err))
        .spawn()
    {
        Ok(c) => c,
        Err(_) => {
            let _ = std::fs::remove_file(&cfg_path);
            let _ = std::fs::remove_file(&log_path);
            return fail;
        }
    };
    if !crate::run::wait_for_ports(&[port], 5000).await {
        let _ = child.kill().await;
        let _ = child.wait().await;
        let _ = std::fs::remove_file(&cfg_path);
        let _ = std::fs::remove_file(&log_path);
        return fail;
    }
    let proxy_url = socks_proxy_url(port);
    // 先抓 probe_url（默认 www.google.com 首页，顺带算速度=大小/耗时）；
    // 首页失败则回退 generate_204 保活（速度记空），提高可用发现率
    let probe = http_get_via_socks(&proxy_url, probe_url, timeout_secs).await;
    let mut result = fail.clone();
    let mut homepage_ok = false;
    if let Some((status, bytes_len, latency)) = probe
        && is_probe_success(status)
    {
        result.alive = true;
        result.delay_ms = latency.max(1);
        result.speed_kbps = calc_speed_kbps(bytes_len, latency);
        homepage_ok = true;
    }
    if !homepage_ok
        && let Some((status, latency)) = http_get_via_socks(
            &proxy_url,
            "https://www.google.com/generate_204",
            timeout_secs.min(8),
        )
        .await
        .map(|(s, _, l)| (s, l))
        && is_probe_success(status)
        && !result.alive
    {
        result.alive = true;
        result.delay_ms = latency.max(1);
    }
    if result.alive
        && let Some((ip, cc)) =
            crate::ipinfo::fetch_ip_via_proxy(&proxy_url, ip_api_url, timeout_secs.min(8)).await
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
                if s > bs || (s == bs && n.delay_ms < b.delay_ms) {
                    best = Some(n);
                }
            }
            None => best = Some(n),
        }
    }
    best
}

/// 并发探测一个批次，就地更新 nodes（选优统一由 select::node_score 在上层判定）
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
