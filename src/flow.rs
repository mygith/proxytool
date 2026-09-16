use anyhow::{anyhow, Result};
use chrono::Utc;
use std::collections::{HashMap, HashSet};

use crate::ctx::Ctx;
use crate::model::Node;
use crate::select::{merge_subscription_nodes, node_matches, sort_nodes_by_delay};
use crate::{config, ipinfo, store, sub, tester};
use crate::say;

pub async fn sub_update(ctx: &Ctx, subs_path: &std::path::Path, name: Option<String>) -> Result<()> {
    let subs = store::load_subs_config(subs_path)?;
    let targets: Vec<(String, String)> = subs
        .into_iter()
        .filter(|(n, _)| name.as_ref().is_none_or(|f| n == f))
        .collect();
    if targets.is_empty() {
        return Err(anyhow!("无匹配订阅（检查 subs.json 与 --name）"));
    }
    // 只把抓取成功的订阅纳入合并范围：失败的若也纳入，其旧节点会被整批剔除
    let mut ok_names: HashSet<String> = HashSet::new();
    let mut failed: Vec<String> = Vec::new();
    let mut all_nodes = Vec::new();
    for (sub_name, url) in &targets {
        say!("更新 {sub_name} -> {url}");
        let parsed = match sub::fetch_subscription(url).await {
            Ok(raw) => {
                // 解析后已自动去重（uri + ip:port）
                let (nodes, stats) = sub::parse_subscription_content(&raw, sub_name);
                say!(
                    "  解析 raw={} uri去重后={} ip:port去重后={}",
                    stats.raw, stats.uri_unique, stats.endpoint_unique
                );
                nodes
            }
            Err(e) => {
                failed.push(format!("{sub_name}: {e:#}"));
                say!("  [警告] 抓取失败，跳过（保留旧节点）: {e:#}");
                continue;
            }
        };
        if parsed.is_empty() {
            failed.push(format!("{sub_name}: 解析出 0 个节点"));
            say!("  [警告] 解析出 0 节点，跳过（保留旧节点）");
            continue;
        }
        ok_names.insert(sub_name.clone());
        all_nodes.extend(parsed);
    }
    if ok_names.is_empty() {
        return Err(anyhow!(
            "全部 {} 个订阅更新失败:\n{}",
            targets.len(),
            failed.join("\n")
        ));
    }
    let mut removed = 0usize;
    ctx.replace_all(|st| {
        let (merged, rm) = merge_subscription_nodes(
            std::mem::take(&mut st.nodes),
            all_nodes.clone(),
            &ok_names,
        );
        removed = rm;
        st.nodes = merged;
        let now = Utc::now();
        for sub_name in &ok_names {
            if let Some(s) = st.subs.iter_mut().find(|x| &x.name == sub_name) {
                s.updated_at = Some(now);
            } else {
                st.subs.push(crate::model::SubMeta {
                    name: sub_name.clone(),
                    updated_at: Some(now),
                });
            }
        }
    })
    .await?;
    let total = ctx.snapshot().await.nodes.len();
    if removed > 0 {
        say!("  跨订阅 ip:port 去重 -{removed}（结果 {total}）");
    }
    for f in &failed {
        say!("[警告] {f}");
    }
    if failed.is_empty() {
        say!("更新完成，共 {total} 节点");
    } else {
        say!(
            "更新完成（部分失败 {}/{}），共 {total} 节点",
            failed.len(),
            targets.len()
        );
    }
    Ok(())
}

pub async fn test(
    ctx: &Ctx,
    mode: &str,
    concurrency: usize,
    timeout: u64,
    with_ipinfo: bool,
    filter: Option<String>,
    top: usize,
) -> Result<()> {
    let settings = ctx.settings().await;
    let (subset, count) = {
        let st = ctx.state.read().await;
        if st.nodes.is_empty() {
            return Err(anyhow!("无节点，请先 sub update"));
        }
        let mut indices: Vec<usize> = (0..st.nodes.len()).collect();
        if let Some(f) = &filter {
            let re = regex::Regex::new(f).map_err(|e| anyhow!("filter regex {e}"))?;
            indices.retain(|&i| node_matches(&st.nodes[i], &re));
            if indices.is_empty() {
                return Err(anyhow!("过滤后无节点"));
            }
        }
        let subset: Vec<Node> = indices.iter().map(|&i| st.nodes[i].clone()).collect();
        (subset, indices.len())
    };
    say!("测速 mode={mode} 并发={concurrency} 超时={timeout}s 节点={count} top={top}");
    let mut subset = subset;
    match mode {
        "tcping" => tester::test_nodes_tcping(&mut subset, concurrency, timeout).await,
        "realping" => {
            tester::test_nodes_realping(
                &mut subset,
                concurrency,
                timeout,
                with_ipinfo,
                &settings.ip_api_url,
                &settings.probe_url,
            )
            .await?;
        }
        "hybrid" => {
            tester::test_nodes_tcping(&mut subset, concurrency, timeout).await;
            subset.sort_by_key(crate::select::delay_rank);
            let mut top_nodes: Vec<Node> = subset
                .iter()
                .filter(|n| n.alive)
                .take(top)
                .cloned()
                .collect();
            if top_nodes.is_empty() {
                say!("tcping 无存活，跳过 realping");
            } else {
                say!("hybrid 第二阶段 realping {} 节点", top_nodes.len());
                tester::test_nodes_realping(
                    &mut top_nodes,
                    concurrency.min(16),
                    timeout,
                    with_ipinfo,
                    &settings.ip_api_url,
                    &settings.probe_url,
                )
                .await?;
                let updates: HashMap<String, Node> =
                    top_nodes.into_iter().map(|n| (n.id.clone(), n)).collect();
                for node in &mut subset {
                    if let Some(updated) = updates.get(&node.id) {
                        *node = updated.clone();
                    }
                }
            }
        }
        _ => return Err(anyhow!("mode 仅支持 tcping/realping/hybrid")),
    }
    ctx.upsert_nodes(&subset).await?;
    ctx.replace_all(|st| sort_nodes_by_delay(&mut st.nodes)).await?;
    let st = ctx.snapshot().await;
    let alive = st.nodes.iter().filter(|n| n.alive).count();
    say!("测速完成 存活 {alive}/{} 已按 delay 排序", st.nodes.len());
    Ok(())
}

/// prune 保留判定（纯函数，便于测试）
///
/// 四条互斥规则，按优先级：
/// 1. 正在被代理使用 -> 留。删了会留下悬空 `node_id`，status 与看护都会失准
/// 2. 延迟达标 -> 留
/// 3. 未测过（无 `last_test_at`）-> 留。delay_ms=-1 既是新节点初值又是失败值，
///    只有 `last_test_at` 能区分二者，否则新拉的订阅会被整批删掉
/// 4. 测过但失败 -> 只有显式 --drop-dead 才删。单次探测失败不等于节点报废
///    （目标站点限流、临时网络抖动都会导致假阴性）
pub const fn keep_in_prune(n: &Node, delay_threshold: i32, drop_dead: bool, in_use: bool) -> bool {
    if in_use || n.delay_ms > delay_threshold {
        return true;
    }
    if n.last_test_at.is_none() {
        return true;
    }
    !drop_dead
}

pub async fn prune(
    ctx: &Ctx,
    delay_threshold: i32,
    keep_top: Option<usize>,
    dedup_endpoint: bool,
    invalid: bool,
    drop_dead: bool,
) -> Result<()> {
    let mut before = 0usize;
    let mut invalid_removed = 0usize;
    let mut dedup_removed = 0usize;
    let mut dead_removed = 0usize;
    let mut after = 0usize;
    ctx.replace_all(|st| {
        before = st.nodes.len();
        let in_use: HashSet<String> = st.running.iter().map(|r| r.node_id.clone()).collect();
        if invalid {
            let bi = st.nodes.len();
            st.nodes
                .retain(|n| !config::is_bogus_endpoint(&n.addr, n.port));
            invalid_removed = bi - st.nodes.len();
        }
        if dedup_endpoint {
            let (deduped, removed) = sub::dedup_by_endpoint(std::mem::take(&mut st.nodes));
            st.nodes = deduped;
            dedup_removed = removed;
        }
        let bd = st.nodes.len();
        st.nodes
            .retain(|n| keep_in_prune(n, delay_threshold, drop_dead, in_use.contains(&n.id)));
        dead_removed = bd - st.nodes.len();
        if let Some(k) = keep_top
            && st.nodes.len() > k
        {
            sort_nodes_by_delay(&mut st.nodes);
            // 前 k 名之外的运行中节点补回来：结果可能略多于 k，但绝不留悬空引用
            let (head, tail) = st.nodes.split_at(k);
            let mut kept = head.to_vec();
            kept.extend(tail.iter().filter(|n| in_use.contains(&n.id)).cloned());
            st.nodes = kept;
        }
        after = st.nodes.len();
    })
    .await?;
    if invalid && invalid_removed > 0 {
        say!("畸形节点清理 -{invalid_removed}（内网地址/端口 0）");
    }
    if dedup_endpoint && dedup_removed > 0 {
        say!("ip:port 去重 -{dedup_removed}");
    }
    if drop_dead {
        say!("失败节点清理 -{dead_removed}（--drop-dead）");
    }
    let policy = if drop_dead {
        "删失败节点"
    } else {
        "保留未测与失败节点"
    };
    say!("prune {before} -> {after} 阈值>{delay_threshold} keep_top={keep_top:?}（{policy}）");
    Ok(())
}

pub async fn ipinfo(ctx: &Ctx, concurrency: usize) -> Result<()> {
    let (mut nodes, api) = {
        let st = ctx.state.read().await;
        (st.nodes.clone(), st.settings.ip_api_url.clone())
    };
    ipinfo::fetch_ipinfo_for_nodes(&mut nodes, concurrency, &api).await;
    ctx.upsert_nodes(&nodes).await?;
    let st = ctx.snapshot().await;
    say!("ipinfo 更新完成");
    for n in st.nodes.iter().filter(|n| n.alive).take(20) {
        say!(
            "[{}] {}:{} -> {} {}",
            n.sub,
            n.addr,
            n.port,
            n.exit_ip.as_deref().unwrap_or("-"),
            n.cc.as_deref().unwrap_or("-")
        );
    }
    Ok(())
}

#[cfg(test)]
mod prune_tests {
    use super::keep_in_prune;
    use crate::model::{Node, NodeType};

    fn node(delay_ms: i32, tested: bool) -> Node {
        let mut n = Node::new("s", NodeType::Vless, "1.1.1.1", 443, "vless://u@1.1.1.1:443");
        n.delay_ms = delay_ms;
        n.last_test_at = tested.then(chrono::Utc::now);
        n
    }

    #[test]
    fn test_keep_fresh_nodes_regardless_of_drop_dead() {
        // 新拉的订阅节点 delay_ms=-1 但没测过，任何模式下都不能删
        for drop_dead in [false, true] {
            assert!(keep_in_prune(&node(-1, false), 0, drop_dead, false));
        }
    }

    #[test]
    fn test_keep_dead_nodes_unless_drop_dead() {
        let dead = node(-1, true);
        assert!(keep_in_prune(&dead, 0, false, false), "缺省保留测过失败的节点");
        assert!(!keep_in_prune(&dead, 0, true, false), "--drop-dead 才删");
    }

    #[test]
    fn test_keep_alive_nodes_by_threshold() {
        assert!(keep_in_prune(&node(120, true), 0, true, false));
        // 阈值以下（含等于）算不达标
        assert!(!keep_in_prune(&node(120, true), 120, true, false));
        assert!(keep_in_prune(&node(121, true), 120, true, false));
    }

    #[test]
    fn test_keep_in_use_node_even_when_dead() {
        // 正在跑的节点即使标死也不能删，否则 running 留悬空 node_id
        assert!(keep_in_prune(&node(-1, true), 0, true, true));
    }
}
