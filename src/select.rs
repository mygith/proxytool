use anyhow::{anyhow, Result};
use std::collections::{HashMap, HashSet};

use crate::model::{AppState, Node, RunningProxy};
use crate::{run, sub};

/// filter 正则匹配 sub/协议/地址/凭证
pub fn node_matches(n: &Node, re: &regex::Regex) -> bool {
    re.is_match(&n.sub)
        || re.is_match(n.r#type.as_str())
        || re.is_match(&n.addr)
        || re.is_match(&n.cred)
}

/// 排序：存活按延迟升序；保活型（probe 过、活着但打不开首页）降权沉底，死节点自然垫底
pub fn sort_nodes_by_delay(nodes: &mut [Node]) {
    nodes.sort_by_key(|n| {
        let delay = if n.alive && n.delay_ms > 0 {
            n.delay_ms
        } else {
            99999
        };
        (n.is_fallback_only(), delay)
    });
}

pub fn parse_ports(value: &str) -> Result<Vec<u16>> {
    let mut ports = Vec::new();
    for raw in value.split(',') {
        let raw = raw.trim();
        if raw.is_empty() {
            return Err(anyhow!("端口列表包含空值"));
        }
        let port = raw.parse::<u16>().map_err(|_| anyhow!("非法端口: {raw}"))?;
        if port == 0 {
            return Err(anyhow!("端口必须在 1..=65535: {raw}"));
        }
        if ports.contains(&port) {
            return Err(anyhow!("端口重复: {port}"));
        }
        ports.push(port);
    }
    if ports.is_empty() {
        return Err(anyhow!("端口列表不能为空"));
    }
    Ok(ports)
}

pub fn validate_strategy(strategy: &str) -> Result<()> {
    match strategy {
        "least-latency" | "random" => Ok(()),
        other => Err(anyhow!(
            "不支持的 strategy: {other}（仅支持 least-latency/random）"
        )),
    }
}

pub fn validate_switch_selector(which: &str) -> Result<()> {
    match which {
        "next" | "prev" | "random" => Ok(()),
        value
            if value
                .strip_prefix("index:")
                .is_some_and(|index| index.parse::<usize>().is_ok()) =>
        {
            Ok(())
        }
        other => Err(anyhow!("不支持的 switch 选择器: {other}")),
    }
}

/// stop 目标扩展：共享同一 pid 的端口连带停止
pub fn expand_stop_indices(running: &[RunningProxy], selected: &[usize]) -> Vec<usize> {
    let pids: HashSet<u32> = selected
        .iter()
        .filter_map(|&idx| running.get(idx).map(|r| r.pid))
        .filter(|&pid| pid != 0)
        .collect();
    let mut expanded: Vec<usize> = running
        .iter()
        .enumerate()
        .filter(|(idx, r)| pids.contains(&r.pid) || selected.contains(idx))
        .map(|(idx, _)| idx)
        .collect();
    expanded.sort_unstable();
    expanded
}

/// 同进程端口组扩展：多端口共享同一 sing-box 进程（同一 pid）时，停/起必须整组一起，
/// 否则杀掉整组进程却只重拉一个端口，其余端口变“假活”。pid=0（无实际进程）不参与分组。
pub fn expand_pid_group(running: &[RunningProxy], ports: &[u16]) -> Vec<u16> {
    let mut pids: HashSet<u32> = HashSet::new();
    for r in running {
        if ports.contains(&r.port) && r.pid != 0 {
            pids.insert(r.pid);
        }
    }
    let mut out: Vec<u16> = running
        .iter()
        .filter(|r| ports.contains(&r.port) || pids.contains(&r.pid))
        .map(|r| r.port)
        .collect();
    out.sort_unstable();
    out.dedup();
    out
}

/// stop 目标端口推导（纯函数）：下标选择 + 共享 pid 扩展 + 同进程组扩展
pub fn stop_target_ports(
    running: &[RunningProxy],
    port: Option<u16>,
    all: bool,
) -> Result<Vec<u16>> {
    let ports_all: Vec<u16> = running.iter().map(|r| r.port).collect();
    let targets = run::select_stop_indices(&ports_all, port, all)?;
    let targets = expand_stop_indices(running, &targets);
    let selected: Vec<u16> = targets
        .iter()
        .filter_map(|&i| running.get(i).map(|r| r.port))
        .collect();
    Ok(expand_pid_group(running, &selected))
}
/// 订阅合并：目标订阅整替、其余保留，跨订阅端点去重
pub fn merge_subscription_nodes(
    existing: Vec<Node>,
    incoming: Vec<Node>,
    target_names: &HashSet<String>,
) -> (Vec<Node>, usize) {
    let mut old_by_id: HashMap<String, Node> = existing
        .into_iter()
        .map(|node| (node.id.clone(), node))
        .collect();
    let mut map = HashMap::new();
    for (id, node) in old_by_id.iter() {
        if !target_names.contains(&node.sub) {
            map.insert(id.clone(), node.clone());
        }
    }
    for node in incoming {
        if let Some(mut old) = old_by_id.remove(&node.id) {
            old.sub = node.sub;
            map.insert(old.id.clone(), old);
        } else {
            map.entry(node.id.clone()).or_insert(node);
        }
    }

    let mut merged: Vec<Node> = map.into_values().collect();
    merged.sort_by(|a, b| {
        (a.sub.is_empty(), &a.sub, &a.id).cmp(&(b.sub.is_empty(), &b.sub, &b.id))
    });
    let before = merged.len();
    let (merged, _) = sub::dedup_by_endpoint(merged);
    let removed = before.saturating_sub(merged.len());
    (merged, removed)
}

/// 选优单一真源：吞吐为主、延迟折算惩罚（延迟每 SCORE_REF_MS 让等效吞吐减半）
/// 例：37.4KB/s@2245ms=11.5 < 22KB/s@500ms=14.7，后者更优
pub const SCORE_REF_MS: f64 = 1000.0;

/// 综合评分；无速度（未 probe 或打不开首页）恒 0，不参与选优
pub fn node_score(n: &Node) -> f64 {
    let delay = if n.delay_ms > 0 {
        n.delay_ms as f64
    } else {
        9999.0
    };
    n.speed_kbps.unwrap_or(0.0) * SCORE_REF_MS / (SCORE_REF_MS + delay)
}

/// 新评分超出在役该倍率才替换（默认 1.10；在役无评分时有评分即换）
pub fn should_replace(old_score: f64, new_score: f64, ratio: f64) -> bool {
    if new_score <= 0.0 {
        return false;
    }
    if old_score <= 0.0 {
        return true;
    }
    new_score > old_score * ratio.max(1.0)
}

/// 看护候选排序：首页可用按综合评分降序，其余存活按延迟升序（纯函数）
pub fn sort_watch_candidates(nodes: &mut [Node]) {
    nodes.sort_by(|a, b| {
        let ah = a.is_homepage_ok();
        let bh = b.is_homepage_ok();
        match (ah, bh) {
            (true, false) => std::cmp::Ordering::Less,
            (false, true) => std::cmp::Ordering::Greater,
            (true, true) => {
                let ord = node_score(b)
                    .partial_cmp(&node_score(a))
                    .unwrap_or(std::cmp::Ordering::Equal);
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
                a.delay_ms.cmp(&b.delay_ms)
            }
            (false, false) => {
                let ad = if a.alive && a.delay_ms > 0 { a.delay_ms } else { 99999 };
                let bd = if b.alive && b.delay_ms > 0 { b.delay_ms } else { 99999 };
                ad.cmp(&bd)
            }
        }
    });
}

/// 仅 2xx/3xx 视为可用，4xx/5xx 一律失败（目标拒绝某出口也算该节点不可用）
/// 单一真源：委托 tester::is_probe_success，勿另起判定
pub fn verify_status_ok(status: u16) -> bool {
    crate::tester::is_probe_success(status)
}

/// 单端口选下一节点（纯函数，便于测试）
pub fn pick_next_node(which: &str, alive: &[Node], cur_id: &str) -> Node {
    use rand::seq::IndexedRandom;
    let cur_pos = alive.iter().position(|x| x.id == cur_id).unwrap_or(0);
    let node = match which {
        "prev" => &alive[(cur_pos + alive.len() - 1) % alive.len()],
        "random" => alive.choose(&mut rand::rng()).unwrap(),
        s if s.starts_with("index:") => {
            let idx: usize = s.strip_prefix("index:").unwrap_or(s).parse().unwrap_or(0);
            &alive[idx % alive.len()]
        }
        _ => &alive[(cur_pos + 1) % alive.len()], // next 及兜底
    };
    node.clone()
}

/// --all 整体轮换：从当前首端口节点位置向后顺延 n 个（纯函数）
pub fn rotate_node_ids(alive: &[Node], cur_ids: &[String], n: usize) -> Vec<String> {
    let cur_first = cur_ids
        .first()
        .and_then(|id| alive.iter().position(|x| &x.id == id))
        .unwrap_or(0);
    let start = (cur_first + n) % alive.len().max(1);
    (0..n)
        .map(|i| alive[(start + i) % alive.len()].id.clone())
        .collect()
}

/// 单端口选择（纯函数）：指定则用之；未指定且仅 1 个在运行则默认用它
pub fn select_switch_port(ports: &[u16], port: Option<u16>) -> Result<u16> {
    if let Some(p) = port {
        return Ok(p);
    }
    if ports.len() == 1 {
        return Ok(ports[0]);
    }
    Err(anyhow!(
        "当前 {} 个代理在运行，请指定 --port <port> 或 --all",
        ports.len()
    ))
}

/// status 行渲染（含看护健康标记：✓ 最近一次实测通过 / ✗ 连续失败）
pub fn describe_running(st: &AppState, r: &RunningProxy) -> String {
    let alive = run::is_pid_alive(r.pid);
    let node_info = st
        .nodes
        .iter()
        .find(|x| x.id == r.node_id)
        .map(|n| {
            format!(
                "[{}] {}:{} {}ms {:.1}KB/s ip={} cc={}",
                n.sub,
                n.addr,
                n.port,
                n.delay_ms,
                n.speed_kbps.unwrap_or(0.0),
                n.exit_ip.as_deref().unwrap_or("-"),
                n.cc.as_deref().unwrap_or("-")
            )
        })
        .unwrap_or_else(|| format!("node={}", r.node_id));
    let watch = match st.meta.get(&crate::model::watch_key(r.port)) {
        Some(_) => {
            // 配置存在即有看护；健康状态读独立 key（缺失=首轮未测）
            let mut tag = " 看护=server 已启动".to_string();
            if let Some(json) = st.meta.get(&crate::model::watch_status_key(r.port))
                && let Ok(s) = serde_json::from_str::<crate::model::WatchStatus>(json)
            {
                match (s.last_ok, s.last_check) {
                    (Some(true), Some(t)) => {
                        tag = format!(" 看护=server ✓ {}s前", (chrono::Utc::now() - t).num_seconds().max(0));
                    }
                    (Some(true), None) => {
                        tag = " 看护=server ✓".to_string();
                    }
                    (Some(false), _) => {
                        tag = format!(" 看护=server ✗ 连续失败{}", s.fail_count);
                    }
                    _ => {}
                }
            }
            tag
        }
        None => String::new(),
    };
    format!(
        "  {} -> {} pid={} {} log={}{watch}",
        r.port,
        node_info,
        r.pid,
        if alive { "运行中" } else { "已退出" },
        r.log_path
    )
}

pub fn running_ports(st: &AppState) -> Vec<u16> {
    let mut ports: Vec<u16> = st.running.iter().map(|r| r.port).collect();
    ports.sort_unstable();
    ports
}

pub fn running_node_id(st: &AppState, port: u16) -> String {
    st.running
        .iter()
        .find(|r| r.port == port)
        .map(|r| r.node_id.clone())
        .unwrap_or_default()
}

/// 解析 ports 对应的映射节点（任一缺失即报错）
pub fn resolve_mapped_nodes(st: &AppState, ports: &[u16]) -> Result<Vec<Node>> {
    ports
        .iter()
        .map(|p| {
            let id = running_node_id(st, *p);
            st.nodes
                .iter()
                .find(|n| n.id == id)
                .cloned()
                .ok_or_else(|| anyhow!("端口 {p} 映射的节点 {id} 不存在"))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tn(id: &str) -> Node {
        let mut n = Node::new(
            "s",
            crate::model::NodeType::Vless,
            "1.1.1.1",
            443,
            &format!("vless://u@1.1.1.1:443#{id}"),
        );
        n.id = id.to_string();
        n.alive = true;
        n
    }

    #[test]
    fn test_sort_nodes_by_delay_demotes_fallback_only() {
        let mut ok = tn("ok");
        ok.delay_ms = 500;
        ok.speed_kbps = Some(10.0);
        ok.probed = true;
        let mut tcping_pool = tn("tcping");
        tcping_pool.delay_ms = 100;
        // 保活型：延迟最低也沉底，且不会被删
        let mut fallback = tn("fallback");
        fallback.delay_ms = 10;
        fallback.probed = true;
        assert!(fallback.is_fallback_only());
        let mut v = vec![fallback.clone(), ok.clone(), tcping_pool.clone()];
        sort_nodes_by_delay(&mut v);
        let ids: Vec<&str> = v.iter().map(|n| n.id.as_str()).collect();
        assert_eq!(ids, vec!["tcping", "ok", "fallback"]);
        assert_eq!(v.len(), 3, "保活型只降权，不得删除");
    }

    #[test]
    fn test_parse_ports_rejects_invalid_empty_and_duplicate_values() {
        assert!(parse_ports("10808,broken").is_err());
        assert!(parse_ports("").is_err());
        assert!(parse_ports("10808,10808").is_err());
        assert_eq!(parse_ports("10808,10809").unwrap(), vec![10808, 10809]);
        assert!(validate_strategy("nope").is_err());
        assert!(validate_switch_selector("index:nope").is_err());
        assert!(validate_switch_selector("index:3").is_ok());
    }

    #[test]
    fn test_expand_stop_indices_groups_shared_pid() {
        let running = vec![
            RunningProxy {
                port: 10808,
                node_id: "a".into(),
                pid: 7,
                config_path: "a".into(),
                log_path: "a".into(),
                started_at: None,
            },
            RunningProxy {
                port: 10809,
                node_id: "b".into(),
                pid: 7,
                config_path: "a".into(),
                log_path: "a".into(),
                started_at: None,
            },
            RunningProxy {
                port: 10810,
                node_id: "c".into(),
                pid: 8,
                config_path: "b".into(),
                log_path: "b".into(),
                started_at: None,
            },
        ];
        assert_eq!(expand_stop_indices(&running, &[0]), vec![0, 1]);
    }

    #[test]
    fn test_subscription_update_replaces_target_subscription_nodes() {
        let old_target = Node::new(
            "sub-a",
            crate::model::NodeType::Vless,
            "1.1.1.1",
            443,
            "vless://old@1.1.1.1:443",
        );
        let other = Node::new(
            "sub-b",
            crate::model::NodeType::Vless,
            "2.2.2.2",
            443,
            "vless://other@2.2.2.2:443",
        );
        let incoming = Node::new(
            "sub-a",
            crate::model::NodeType::Vless,
            "3.3.3.3",
            443,
            "vless://new@3.3.3.3:443",
        );
        let targets: HashSet<String> = ["sub-a".to_string()].into_iter().collect();
        let (merged, _) =
            merge_subscription_nodes(vec![old_target, other], vec![incoming], &targets);
        assert_eq!(merged.len(), 2);
        assert!(merged.iter().any(|node| node.addr == "3.3.3.3"));
        assert!(!merged.iter().any(|node| node.addr == "1.1.1.1"));
        assert!(merged.iter().any(|node| node.addr == "2.2.2.2"));
    }

    #[test]
    fn test_should_replace_needs_ten_percent() {
        // 超 10% 才换
        assert!(should_replace(100.0, 111.0, 1.10));
        assert!(!should_replace(100.0, 105.0, 1.10));
        assert!(!should_replace(100.0, 110.0, 1.10));
        assert!(!should_replace(100.0, 0.0, 1.10));
        // 在役无速度时有速度即换
        assert!(should_replace(0.0, 10.0, 1.10));
    }

    #[test]
    fn test_node_score_penalizes_latency() {
        // 故障实测：高速高延迟 37.4KB/s@2245ms 输给 30KB/s@400ms
        let mut fast_slow_link = tn("a");
        fast_slow_link.speed_kbps = Some(37.4);
        fast_slow_link.delay_ms = 2245;
        let mut slow_fast_link = tn("b");
        slow_fast_link.speed_kbps = Some(30.0);
        slow_fast_link.delay_ms = 400;
        assert!(node_score(&slow_fast_link) > node_score(&fast_slow_link));
        // 无速度恒 0（未 probe / 打不开首页），不参与选优
        let mut unprobed = tn("c");
        unprobed.delay_ms = 181;
        assert_eq!(node_score(&unprobed), 0.0);
    }

    #[test]
    fn test_sort_watch_candidates_prefers_low_latency() {
        fn homepage(id: &str, speed: Option<f64>, delay: i32) -> Node {
            let mut n = tn(id);
            n.speed_kbps = speed;
            n.delay_ms = delay;
            n.probed = true;
            n.alive = speed.is_some() || delay > 0;
            n
        }
        let mut v = vec![
            homepage("high_speed_slow_link", Some(60.0), 3000),
            homepage("balanced", Some(30.0), 200),
        ];
        sort_watch_candidates(&mut v);
        assert_eq!(v[0].id, "balanced");
    }

    #[test]
    fn test_sort_watch_candidates_homepage_speed_first() {
        fn homepage(id: &str, speed: Option<f64>, delay: i32) -> Node {
            let mut n = tn(id);
            n.speed_kbps = speed;
            n.delay_ms = delay;
            n.probed = true;
            n.alive = speed.is_some() || delay > 0;
            n
        }
        let mut v = vec![
            homepage("slow", Some(100.0), 50),
            homepage("fast", Some(500.0), 300),
            homepage("fallback", None, 20),
        ];
        sort_watch_candidates(&mut v);
        assert_eq!(v[0].id, "fast");
        assert_eq!(v[1].id, "slow");
        assert_eq!(v[2].id, "fallback");
    }

    #[test]
    fn test_pick_next_node_cycles_and_index() {
        let alive = vec![tn("n1"), tn("n2"), tn("n3")];
        // next 环形前进；未知当前节点按位置 0 起（下一个为 n2）
        assert_eq!(pick_next_node("next", &alive, "n1").id, "n2");
        assert_eq!(pick_next_node("next", &alive, "n3").id, "n1");
        assert_eq!(pick_next_node("next", &alive, "ghost").id, "n2");
        // prev 环形后退
        assert_eq!(pick_next_node("prev", &alive, "n1").id, "n3");
        // index 取模
        assert_eq!(pick_next_node("index:1", &alive, "n1").id, "n2");
        assert_eq!(pick_next_node("index:5", &alive, "n1").id, "n3");
    }

    #[test]
    fn test_pick_next_node_random_returns_member() {
        let alive = vec![tn("n1"), tn("n2")];
        let picked = pick_next_node("random", &alive, "n1");
        assert!(alive.iter().any(|n| n.id == picked.id));
    }

    #[test]
    fn test_select_switch_port_defaults_to_single_running() {
        // 未指定且仅 1 个在运行 -> 默认它
        assert_eq!(select_switch_port(&[10808], None).unwrap(), 10808);
        // 显式指定优先
        assert_eq!(select_switch_port(&[10808], Some(10809)).unwrap(), 10809);
        // 多个在运行且未指定 -> 报错
        assert!(select_switch_port(&[10808, 10809], None).is_err());
    }

    #[test]
    fn test_rotate_node_ids_wraps() {
        let alive = vec![tn("n1"), tn("n2"), tn("n3"), tn("n4")];
        // 当前首端口 n1，2 端口整体轮换 -> 从 n3 起连取 2 个
        assert_eq!(
            rotate_node_ids(&alive, &["n1".into(), "n2".into()], 2),
            vec!["n3".to_string(), "n4".to_string()]
        );
        // 环形回绕
        assert_eq!(
            rotate_node_ids(&alive, &["n3".into(), "n4".into()], 2),
            vec!["n1".to_string(), "n2".to_string()]
        );
        // 未知当前节点 -> 按位置 0 顺延 n 个
        assert_eq!(
            rotate_node_ids(&alive, &["ghost".into()], 2),
            vec!["n3".to_string(), "n4".to_string()]
        );
    }

    #[test]
    fn test_expand_pid_group() {
        let running = vec![
            RunningProxy {
                port: 10808,
                node_id: "a".into(),
                pid: 7,
                config_path: "a".into(),
                log_path: "a".into(),
                started_at: None,
            },
            RunningProxy {
                port: 10809,
                node_id: "b".into(),
                pid: 7,
                config_path: "a".into(),
                log_path: "a".into(),
                started_at: None,
            },
            RunningProxy {
                port: 10810,
                node_id: "c".into(),
                pid: 8,
                config_path: "b".into(),
                log_path: "b".into(),
                started_at: None,
            },
        ];
        // 同 pid 整组扩展
        assert_eq!(expand_pid_group(&running, &[10808]), vec![10808, 10809]);
        // 独立进程不受影响
        assert_eq!(expand_pid_group(&running, &[10810]), vec![10810]);
        // 不在运行中的端口：无组可扩（调用方回退自身）
        assert_eq!(expand_pid_group(&running, &[19999]), Vec::<u16>::new());
        // stop 目标推导含整组
        assert_eq!(
            stop_target_ports(&running, Some(10808), false).unwrap(),
            vec![10808, 10809]
        );
        assert_eq!(
            stop_target_ports(&running, None, true).unwrap(),
            vec![10808, 10809, 10810]
        );
    }

    #[test]
    fn test_expand_pid_group_ignores_zero_pid() {
        let running = vec![
            RunningProxy {
                port: 10808,
                node_id: "a".into(),
                pid: 0,
                config_path: String::new(),
                log_path: String::new(),
                started_at: None,
            },
            RunningProxy {
                port: 10809,
                node_id: "b".into(),
                pid: 0,
                config_path: String::new(),
                log_path: String::new(),
                started_at: None,
            },
        ];
        // pid=0 不分组，各管各
        assert_eq!(expand_pid_group(&running, &[10808]), vec![10808]);
    }

    #[test]
    fn test_verify_status_ok() {
        assert!(verify_status_ok(200));
        assert!(verify_status_ok(399));
        assert!(!verify_status_ok(400));
        assert!(!verify_status_ok(500));
    }

    #[test]
    fn test_describe_running_watch_health() {
        let mut st = AppState::default();
        st.nodes.push(tn("n1"));
        st.running.push(RunningProxy {
            port: 10808,
            node_id: "n1".into(),
            pid: 1,
            config_path: String::new(),
            log_path: String::new(),
            started_at: None,
        });
        // 无 watch meta
        let line = describe_running(&st, &st.running[0]);
        assert!(!line.contains("看护"));
        // 仅配置无状态 → 已启动
        st.meta.insert("watch:10808".into(), r#"{"verify_url":"x"}"#.into());
        let line = describe_running(&st, &st.running[0]);
        assert!(line.contains("已启动"), "got: {line}");
        // 状态独立 key：成功
        st.meta.insert(
            "watchstatus:10808".into(),
            r#"{"last_ok":true,"fail_count":0}"#.into(),
        );
        let line = describe_running(&st, &st.running[0]);
        assert!(line.contains("看护=server ✓"), "got: {line}");
        // 状态独立 key：失败态
        st.meta.insert(
            "watchstatus:10808".into(),
            r#"{"last_ok":false,"fail_count":3}"#.into(),
        );
        let line = describe_running(&st, &st.running[0]);
        assert!(line.contains("连续失败3"), "got: {line}");
    }
}
