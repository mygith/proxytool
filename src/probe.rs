use anyhow::{anyhow, Result};
use std::collections::HashMap;

use crate::ctx::Ctx;
use crate::model::Node;
use crate::proxy::{launch_fresh, replace_live};
use crate::select::{node_matches, node_score, should_replace};
use crate::rpc::AutoParams;
use crate::tester;
use crate::say;

/// 已测节点的快照：取 subset 前 end 个，按当前内存态刷新，并剔除探测期间被删的节点
///
/// 按 id 建索引后再查，复杂度 O(N+end)；线性 find 是 O(end×N)。
/// 实测（N=5763、60 批）按当前库形态从约 20ms 降到 8ms，最坏（全部存活、顺序无关）
/// 从约 110ms 降到 8ms。探测整体由网络耗时主导，此处只是顺手消除无谓扫描
fn probed_snapshot(nodes: &[Node], subset: &[Node], end: usize) -> Vec<Node> {
    let by_id: HashMap<&str, &Node> =
        nodes.iter().map(|n| (n.id.as_str(), n)).collect();
    subset[..end]
        .iter()
        .filter_map(|n| by_id.get(n.id.as_str()).map(|x| (*x).clone()))
        .collect()
}

/// 探测引擎参数（独立 probe 与 auto 流式共用）
struct ProbeOpts<'a> {
    batch_size: usize,
    timeout: u64,
    probe_url: &'a str,
    max_batches: usize,
    concurrency: usize,
    filter: Option<String>,
    /// None：全量测完取最快；Some(port)：首个可用即上线，超阈值即替换
    serving_port: Option<u16>,
}

/// 探测引擎：小批量逐批
#[allow(
    clippy::too_many_lines,
    reason = "探测引擎包含批次循环+流式上线逻辑，拆分增加状态传递成本"
)]
#[allow(clippy::significant_drop_tightening, reason = "RwLock 读锁在块作用域内仍需持有到 subset 构建完成")]
async fn probe_engine(ctx: &Ctx, o: ProbeOpts<'_>) -> Result<Option<String>> {
    let ProbeOpts {
        batch_size,
        timeout,
        probe_url,
        max_batches,
        concurrency,
        filter,
        serving_port,
    } = o;
    if tester::singbox_bin().is_none() {
        return Err(anyhow!(
            "未找到 sing-box，请先安装 sing-box 后再 probe（run 同样需要）"
        ));
    }
    let (mut subset, total) = {
        let st = ctx.state.read().await;
        if st.nodes.is_empty() {
            return Err(anyhow!("无节点，请先 sub update"));
        }
        let mut indices: Vec<usize> = (0..st.nodes.len()).collect();
        if let Some(f) = &filter {
            let re = regex::Regex::new(f).map_err(|e| anyhow!("filter regex {e}"))?;
            indices.retain(|&i| node_matches(&st.nodes[i], &re));
        }
        if indices.is_empty() {
            return Err(anyhow!("过滤后无节点"));
        }
        let mut subset: Vec<Node> = indices.iter().map(|&i| st.nodes[i].clone()).collect();
        // 存活优先、保活型降权（打不开首页，别反复占用探测名额）、延迟升序
        subset.sort_by_key(|n| {
            let alive_rank = i32::from(!n.alive);
            (alive_rank, n.is_fallback_only(), crate::select::delay_rank(n))
        });
        let count = subset.len();
        (subset, count)
    };
    let batches = tester::calc_batches(total, batch_size.max(1), Some(max_batches.max(1)));
    say!(
        "probe 目标={probe_url} 超时={timeout}s 总数={total} 批次={} 每批={batch_size} 并发={concurrency}",
        batches.len()
    );
    let settings = ctx.settings().await;
    let ip_api = settings.ip_api_url.clone();
    let ratio = settings.replace_speed_ratio;
    let mut best_overall: Option<(String, f64, i32)> = None;
    let mut serving_id: Option<String> = None;
    let mut serving_score = 0.0f64;
    for (bi, (s, e)) in batches.iter().enumerate() {
        say!("-- 批次 {}/{} [{s}..{e}) --", bi + 1, batches.len());
        let mut batch_nodes: Vec<Node> = subset[*s..*e].to_vec();
        tester::probe_batch(&mut batch_nodes, probe_url, &ip_api, timeout, concurrency).await;
        // 内存与 DB 同步本批结果
        ctx.upsert_nodes(&batch_nodes).await?;
        // 镜像同步本地 subset
        for local in &mut subset[*s..*e] {
            if let Some(u) = batch_nodes.iter().find(|x| x.id == local.id) {
                *local = u.clone();
            }
        }
        let ok_in_batch = subset[*s..*e].iter().filter(|n| n.is_homepage_ok()).count();
        say!("   批次结果: 可用 {ok_in_batch}/{}", e - s);
        let probed = {
            let st = ctx.state.read().await;
            probed_snapshot(&st.nodes, &subset, *e)
        };
        // 选优只算一次，下面两处共用（此前对同一份 probed 各算了一遍）
        let best = tester::pick_best_homepage(&probed);
        if let Some(b) = best {
            let sc = node_score(b);
            let better = match &best_overall {
                Some((_, bsc, bd)) => sc > *bsc || ((sc - *bsc).abs() < f64::EPSILON && b.delay_ms < *bd),
                None => true,
            };
            if better {
                best_overall = Some((b.id.clone(), sc, b.delay_ms));
            }
        }
        // 流式链路：首个可用即上线，后续超阈值即替换
        if let Some(port) = serving_port
            && let Some(cur_best) = best.cloned()
        {
            let cur_score = node_score(&cur_best);
            match &serving_id {
                None => {
                    say!(
                        "   [即时上线] {}ms {:.1}KB/s 评分{:.1} [{}] {}:{}",
                        cur_best.delay_ms,
                        cur_best.speed_kbps.unwrap_or(0.0),
                        cur_score,
                        cur_best.sub,
                        cur_best.addr,
                        cur_best.port
                    );
                    if launch_fresh(ctx, &cur_best, port, probe_url, timeout).await? {
                        serving_id = Some(cur_best.id.clone());
                        serving_score = cur_score;
                        say!("   代理已就绪，后续批次继续探测，更优即替换");
                    }
                }
                Some(cur)
                    if cur != &cur_best.id && should_replace(serving_score, cur_score, ratio) =>
                {
                    say!(
                        "   [替换] 评分 {serving_score:.1} -> {cur_score:.1}（{}ms {:.1}KB/s [{}] {}:{}）",
                        cur_best.delay_ms,
                        cur_best.speed_kbps.unwrap_or(0.0),
                        cur_best.sub,
                        cur_best.addr,
                        cur_best.port
                    );
                    if replace_live(ctx, port, &cur_best.id, probe_url, timeout).await? {
                        serving_id = Some(cur_best.id.clone());
                        serving_score = cur_score;
                    }
                }
                _ => {}
            }
        }
    }
    // 收尾统一排序落盘一次（批内已 upsert 结果，避免每批全表重写 60 次）
    ctx.replace_all(|st| crate::select::sort_nodes_by_delay(&mut st.nodes))
        .await?;
    let found_id = best_overall.map(|(id, _, _)| id);
    if serving_port.is_some() {
        return serving_id.map_or_else(|| Err(anyhow!("probe 未找到可用节点")), |sid| Ok(Some(sid)));
    }
    if let Some(best_id) = &found_id {
        let st = ctx.snapshot().await;
        if let Some(n) = st.nodes.iter().find(|x| &x.id == best_id) {
            say!("全量探测完成，最优节点:");
            say!(
                "可用: [{}] {} {}:{} {}ms {:.1}KB/s ip={} cc={}",
                n.sub,
                n.r#type.as_str(),
                n.addr,
                n.port,
                n.delay_ms,
                n.speed_kbps.unwrap_or(0.0),
                n.exit_ip.as_deref().unwrap_or("-"),
                n.cc.as_deref().unwrap_or("-")
            );
            say!("下一步: proxytool run --port 10808");
        }
    } else {
        say!(
            "所有批次均未找到可用节点（已测 {} 批），建议扩大 max-batches 或更换订阅/过滤条件",
            batches.len()
        );
    }
    Ok(found_id)
}

/// 从 CLI/RPC 的 Option 字段与配置解析出探测参数（CLI 优先，缺省读配置）
#[allow(clippy::ref_option, reason = "与 CLI struct 的 Option<String> 字段对齐，避免调用方额外 .as_ref()")]
fn resolve_probe(
    batch_size: Option<usize>,
    timeout: Option<u64>,
    probe_url: &Option<String>,
    max_batches: Option<usize>,
    concurrency: Option<usize>,
    s: &crate::model::Settings,
) -> (usize, u64, String, usize, usize) {
    (
        batch_size.unwrap_or(s.probe_batch_size),
        timeout.unwrap_or(s.probe_timeout),
        probe_url
            .clone()
            .unwrap_or_else(|| s.probe_url.clone()),
        max_batches.unwrap_or(s.probe_max_batches),
        concurrency.unwrap_or(s.probe_concurrency),
    )
}

/// 独立 probe 命令：全量测完取最快，不做上线动作
pub async fn probe(
    ctx: &crate::ctx::Ctx,
    p: &crate::rpc::ProbeParams,
) -> Result<()> {
    let s = ctx.settings().await;
    let (batch_size, timeout, probe_url, max_batches, concurrency) =
        resolve_probe(p.batch_size, p.timeout, &p.probe_url, p.max_batches, p.concurrency, &s);
    probe_engine(
        ctx,
        ProbeOpts {
            batch_size,
            timeout,
            probe_url: &probe_url,
            max_batches,
            concurrency,
            filter: p.filter.clone(),
            serving_port: None,
        },
    )
    .await
    .map(|_| ())
}

/// 流式探测 + 上线（auto 链路）
pub async fn streaming_probe_and_serve(ctx: &crate::ctx::Ctx, p: &AutoParams) -> Result<()> {
    let s = ctx.settings().await;
    let (batch_size, timeout, probe_url, max_batches, concurrency) = resolve_probe(
        p.batch_size,
        p.probe_timeout,
        &p.probe_url,
        p.max_batches,
        p.probe_concurrency,
        &s,
    );
    probe_engine(
        ctx,
        ProbeOpts {
            batch_size,
            timeout,
            probe_url: &probe_url,
            max_batches,
            concurrency,
            filter: p.filter.clone(),
            serving_port: Some(p.port),
        },
    )
    .await?
    .ok_or_else(|| anyhow!("probe 未找到可用节点"))?;
    Ok(())
}

/// probe 全落空时的兜底候选：存活（可退化为全量）按延迟排序
#[allow(clippy::ref_option, reason = "与 CLI struct 的 Option<String> 字段对齐")]
pub async fn fallback_candidates(ctx: &crate::ctx::Ctx, filter: &Option<String>) -> Vec<Node> {
    let st = ctx.snapshot().await;
    let mut v: Vec<Node> = st.nodes.iter().filter(|n| n.alive).cloned().collect();
    if let Some(f) = filter
        && let Ok(re) = regex::Regex::new(f)
    {
        v.retain(|n| node_matches(n, &re));
    }
    if v.is_empty() {
        v = st.nodes;
    }
    crate::select::sort_nodes_by_delay(&mut v);
    v
}

#[cfg(test)]
mod probed_snapshot_tests {
    use super::*;

    fn node(id: &str, delay_ms: i32) -> Node {
        let mut n = Node::new(
            "sub-a",
            crate::model::NodeType::Vless,
            "1.1.1.1",
            443,
            &format!("vless://u@1.1.1.1:443#{id}"),
        );
        n.id = id.to_string();
        n.alive = true;
        n.delay_ms = delay_ms;
        n
    }

    /// 快照必须以内存态为准：subset 是探测开始时的镜像，期间被 upsert 的结果要能看到
    #[test]
    fn test_probed_snapshot_prefers_memory_state() {
        let subset = vec![node("a", 900), node("b", 800)];
        let nodes = vec![node("a", 100), node("b", 200)];
        let got = probed_snapshot(&nodes, &subset, 2);
        assert_eq!(got.iter().map(|n| n.delay_ms).collect::<Vec<_>>(), vec![100, 200]);
    }

    /// 探测期间被删（或被 prune 掉）的节点必须剔除，否则会拿已不存在的节点去选优
    #[test]
    fn test_probed_snapshot_drops_deleted_nodes() {
        let subset = vec![node("a", 100), node("gone", 50), node("c", 300)];
        let nodes = vec![node("a", 100), node("c", 300)];
        let got = probed_snapshot(&nodes, &subset, 3);
        let ids: Vec<&str> = got.iter().map(|n| n.id.as_str()).collect();
        assert_eq!(ids, vec!["a", "c"]);
    }

    /// 只取已测前缀：未测到的后半段不参与选优
    #[test]
    fn test_probed_snapshot_limits_to_probed_prefix() {
        let subset = vec![node("a", 100), node("b", 200), node("c", 300)];
        let nodes = vec![node("a", 100), node("b", 200), node("c", 300)];
        let got = probed_snapshot(&nodes, &subset, 2);
        let ids: Vec<&str> = got.iter().map(|n| n.id.as_str()).collect();
        assert_eq!(ids, vec!["a", "b"]);
    }

    #[test]
    fn test_probed_snapshot_empty_nodes_drops_everything() {
        let subset = vec![node("a", 100)];
        assert!(probed_snapshot(&[], &subset, 1).is_empty());
    }
}

