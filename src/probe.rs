use anyhow::{anyhow, Result};
use std::collections::HashSet;

use crate::ctx::Ctx;
use crate::model::Node;
use crate::proxy::{launch_fresh, replace_live};
use crate::select::{node_matches, node_score, should_replace};
use crate::rpc::AutoParams;
use crate::{config, tester};
use crate::say;

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
        subset.sort_by_key(|n| {
            let alive_rank = if n.alive { 0 } else { 1 };
            let delay_rank = if n.delay_ms > 0 { n.delay_ms } else { 99999 };
            (alive_rank, delay_rank)
        });
        let count = subset.len();
        (subset, count)
    };
    let batches = tester::calc_batches(total, batch_size.max(1), Some(max_batches.max(1)));
    say!(
        "probe 目标={probe_url} 超时={timeout}s 总数={total} 批次={} 每批={batch_size} 并发={concurrency}",
        batches.len()
    );
    let settings = config::load_or_create()?;
    let ip_api = settings.ip_api_url.clone();
    let ratio = settings.replace_speed_ratio;
    let mut doomed: HashSet<String> = HashSet::new();
    let mut best_overall: Option<(String, f64, i32)> = None;
    let mut serving_id: Option<String> = None;
    let mut serving_score = 0.0f64;
    for (bi, (s, e)) in batches.iter().enumerate() {
        say!("-- 批次 {}/{} [{s}..{e}) --", bi + 1, batches.len());
        let batch_items: Vec<Node> = subset[*s..*e]
            .iter()
            .filter(|n| !doomed.contains(&n.id))
            .cloned()
            .collect();
        if batch_items.is_empty() {
            continue;
        }
        let mut batch_nodes = batch_items;
        tester::probe_batch(&mut batch_nodes, probe_url, &ip_api, timeout, concurrency).await;
        // 内存与 DB 同步本批结果
        ctx.upsert_nodes(&batch_nodes).await?;
        // 镜像同步本地 subset
        for local in subset[*s..*e].iter_mut() {
            if let Some(u) = batch_nodes.iter().find(|x| x.id == local.id) {
                *local = u.clone();
            }
        }
        // 保活型自动删除（本批新测出的才删：probed 由本批 probe_batch 打标）
        let batch_doomed: Vec<String> = subset[*s..*e]
            .iter()
            .filter(|n| n.is_fallback_only() && doomed.insert(n.id.clone()))
            .map(|n| n.id.clone())
            .collect();
        if !batch_doomed.is_empty() {
            ctx.delete_nodes(&batch_doomed).await?;
            say!("   删除保活型 {}（仅保活、打不开首页）", batch_doomed.len());
        }
        let ok_in_batch = subset[*s..*e].iter().filter(|n| n.is_homepage_ok()).count();
        say!("   批次结果: 可用 {ok_in_batch}/{}", e - s);
        // 已测全部的快照（从内存取，剔除已删节点）
        let probed: Vec<Node> = {
            let st = ctx.state.read().await;
            subset[..*e]
                .iter()
                .filter_map(|n| st.nodes.iter().find(|x| x.id == n.id).cloned())
                .collect()
        };
        if let Some(b) = tester::pick_best_homepage(&probed) {
            let sc = node_score(b);
            let better = match &best_overall {
                Some((_, bsc, bd)) => sc > *bsc || (sc == *bsc && b.delay_ms < *bd),
                None => true,
            };
            if better {
                best_overall = Some((b.id.clone(), sc, b.delay_ms));
            }
        }
        // 流式链路：首个可用即上线，后续超阈值即替换
        if let Some(port) = serving_port
            && let Some(cur_best) = tester::pick_best_homepage(&probed).cloned()
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
        return match serving_id {
            Some(sid) => Ok(Some(sid)),
            None => Err(anyhow!("probe 未找到可用节点")),
        };
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

/// 独立 probe 命令：全量测完取最快，不做上线动作
pub async fn probe(
    ctx: &crate::ctx::Ctx,
    batch_size: usize,
    timeout: u64,
    probe_url: &str,
    max_batches: usize,
    concurrency: usize,
    filter: Option<String>,
) -> Result<()> {
    probe_engine(
        ctx,
        ProbeOpts {
            batch_size,
            timeout,
            probe_url,
            max_batches,
            concurrency,
            filter,
            serving_port: None,
        },
    )
    .await
    .map(|_| ())
}

/// 流式探测 + 上线（auto 链路）
pub async fn streaming_probe_and_serve(ctx: &crate::ctx::Ctx, p: &AutoParams) -> Result<()> {
    probe_engine(
        ctx,
        ProbeOpts {
            batch_size: p.batch_size,
            timeout: p.probe_timeout,
            probe_url: &p.probe_url,
            max_batches: p.max_batches,
            concurrency: p.probe_concurrency,
            filter: p.filter.clone(),
            serving_port: Some(p.port),
        },
    )
    .await?
    .ok_or_else(|| anyhow!("probe 未找到可用节点"))?;
    Ok(())
}

/// probe 全落空时的兜底候选：存活（可退化为全量）按延迟排序
pub async fn fallback_candidates(ctx: &crate::ctx::Ctx, filter: &Option<String>) -> Vec<Node> {
    let st = ctx.snapshot().await;
    let mut v: Vec<Node> = st.nodes.iter().filter(|n| n.alive).cloned().collect();
    if let Some(f) = filter
        && let Ok(re) = regex::Regex::new(f)
    {
        v.retain(|n| node_matches(n, &re));
    }
    if v.is_empty() {
        v = st.nodes.clone();
    }
    crate::select::sort_nodes_by_delay(&mut v);
    v
}
