use anyhow::Result;
use std::sync::Arc;

use crate::ctx::Ctx;
use crate::model::{Node, WatchConfig, WatchStatus, watch_key, watch_status_key};
use crate::proxy::{relaunch_from_running, stop_running_processes};
use crate::select::{node_matches, running_node_id, sort_candidates_by_score, verify_status_ok};
use crate::{config, run, tester};
use crate::say;

/// 看护/切换单次最多尝试的候选节点数
pub const SWITCH_MAX_TRIES: usize = 5;

/// 启动/确保某端口的看护任务，并写 meta 便于 serve 重启恢复
pub async fn start_watch(ctx: &Arc<Ctx>, port: u16, cfg: WatchConfig) -> Result<()> {
    {
        let mut watches = ctx.watches.lock().await;
        if let Some(h) = watches.get(&port)
            && !h.is_finished()
        {
            return Ok(());
        }
        let c2 = ctx.clone();
        let cfg2 = cfg.clone();
        let h = tokio::spawn(async move {
            watch_forever(c2, port, cfg2).await;
        });
        watches.insert(port, h);
    }
    ctx.set_meta(&watch_key(port), Some(serde_json::to_string(&cfg)?))
        .await
}

/// 停止某端口的看护任务并清 meta（配置与状态双 key）
pub async fn stop_watch(ctx: &Ctx, port: u16) -> Result<()> {
    if let Some(h) = ctx.watches.lock().await.remove(&port) {
        h.abort();
    }
    ctx.set_meta(&watch_key(port), None).await?;
    ctx.set_meta(&watch_status_key(port), None).await
}

/// serve 重启后按 running 表 + meta watch 配置恢复看护
pub async fn adopt_running(ctx: &Arc<Ctx>) {
    let st = ctx.snapshot().await;
    for r in &st.running {
        if let Some(cfg_json) = st.meta.get(&watch_key(r.port)) {
            match serde_json::from_str::<WatchConfig>(cfg_json) {
                Ok(cfg) => {
                    let _ = start_watch(ctx, r.port, cfg).await;
                }
                Err(e) => say!("看护恢复失败 port={}: {e}", r.port),
            }
        }
    }
}

/// 看护状态回写独立 key（status 展示用；失败不影响看护主流程）
async fn push_watch_status(ctx: &Ctx, port: u16, ok: bool, fails: usize) {
    let s = WatchStatus {
        last_ok: Some(ok),
        last_check: Some(chrono::Utc::now()),
        fail_count: fails,
    };
    let json = serde_json::to_string(&s).unwrap_or_default();
    if let Err(e) = ctx.set_meta(&watch_status_key(port), Some(json)).await {
        say!("看护：状态写回失败 port={port}: {e:#}");
    }
}

/// 常驻看护：周期经代理实测目标网址，连续失败达阈值或进程死亡立即更换
async fn watch_forever(ctx: Arc<Ctx>, port: u16, cfg: WatchConfig) {
    let mut fail_count = 0usize;
    // 冷却起点前移一小时，首个周期即可切换
    let mut last_switch = std::time::Instant::now()
        .checked_sub(std::time::Duration::from_secs(3600))
        .unwrap_or_else(std::time::Instant::now);
    loop {
        let settings = match config::load_or_create() {
            Ok(s) => s,
            Err(e) => {
                say!("看护：配置读取失败，退避重试: {e:#}");
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                continue;
            }
        };
        let interval = settings.watch_interval_secs.max(5);
        tokio::time::sleep(std::time::Duration::from_secs(interval)).await;
        let running = {
            let st = ctx.state.read().await;
            st.running.iter().find(|r| r.port == port).cloned()
        };
        let Some(running) = running else {
            say!("看护退出：端口 {port} 运行态已消失（已 stop）");
            let _ = ctx.set_meta(&watch_key(port), None).await;
            let _ = ctx.set_meta(&watch_status_key(port), None).await;
            return;
        };
        let timeout = settings.watch_timeout_secs.max(5);
        let threshold = settings.watch_fail_threshold.max(1);
        if !run::is_pid_alive(running.pid) {
            // 进程已死是确定性故障：不受失败阈值约束（立即换），但受冷却约束（防连败忙循环）
            fail_count += 1;
            if last_switch.elapsed().as_secs() < settings.watch_cooldown_secs {
                say!(
                    "看护：sing-box pid={} 已死，冷却中（{}s），下周期重试",
                    running.pid, settings.watch_cooldown_secs
                );
                push_watch_status(&ctx, port, false, fail_count).await;
                continue;
            }
            say!("看护：sing-box pid={} 已死，立即更换", running.pid);
            if watch_failover(ctx.clone(), port, &cfg, timeout).await {
                fail_count = 0;
                last_switch = std::time::Instant::now();
                push_watch_status(&ctx, port, true, 0).await;
            } else {
                push_watch_status(&ctx, port, false, fail_count).await;
            }
            continue;
        }
        let proxy = format!("socks5h://127.0.0.1:{port}");
        let ok = tester::http_get_via_socks(&proxy, &cfg.verify_url, timeout, tester::NO_BODY)
            .await
            .is_some_and(|(s, _, _)| verify_status_ok(s));
        if ok {
            fail_count = 0;
            push_watch_status(&ctx, port, true, 0).await;
            continue;
        }
        // 基准不通：再探出口，区分节点假活与目标拒绝该出口
        let ip_ok = tester::http_get_via_socks(&proxy, &settings.ip_api_url, timeout.min(8), tester::NO_BODY)
            .await
            .is_some_and(|(s, _, _)| verify_status_ok(s));
        if ip_ok {
            say!(
                "看护：目标不通但出口可用（目标可能拒绝该出口），计失败 {}/{}",
                fail_count + 1,
                threshold
            );
        } else {
            say!(
                "看护：目标与出口均不通，计失败 {}/{}",
                fail_count + 1,
                threshold
            );
            let _ = ctx.mark_dead(&running.node_id).await;
        }
        fail_count += 1;
        push_watch_status(&ctx, port, false, fail_count).await;
        if fail_count < threshold {
            continue;
        }
        if last_switch.elapsed().as_secs() < settings.watch_cooldown_secs {
            say!("看护：冷却中（{}s），暂不切换", settings.watch_cooldown_secs);
            continue;
        }
        say!("看护：连续失败达阈值，立即更换端口 {port}");
        if watch_failover(ctx.clone(), port, &cfg, timeout).await {
            fail_count = 0;
            last_switch = std::time::Instant::now();
            push_watch_status(&ctx, port, true, 0).await;
        }
    }
}

/// 看护换节点：候选逐个试（换前停旧进程），首个验证通过即生效
/// 同进程整组一起停起；重拉失败用锚点恢复运行行再试下一个；整组加锁防并发拆台
async fn watch_failover(ctx: Arc<Ctx>, port: u16, cfg: &WatchConfig, timeout: u64) -> bool {
    let group: Vec<u16> = {
        let st = ctx.snapshot().await;
        let g = crate::select::expand_pid_group(&st.running, &[port]);
        if g.is_empty() { vec![port] } else { g }
    };
    let _guard = match ctx.acquire_ports(&group) {
        Ok(g) => g,
        Err(_) => {
            say!("看护：端口 {port} 正被其他任务操作，跳过本轮");
            return false;
        }
    };
    let mut pool = {
        let st = ctx.snapshot().await;
        let cur = running_node_id(&st, port);
        let mut pool: Vec<Node> = st.nodes.iter().filter(|n| n.alive).cloned().collect();
        if let Some(f) = &cfg.filter {
            match regex::Regex::new(f) {
                Ok(re) => pool.retain(|n| node_matches(n, &re)),
                Err(e) => say!("看护：filter 非法 {e}，不过滤"),
            }
        }
        pool.retain(|n| n.id != cur);
        if pool.is_empty() {
            // 无存活候选则退回全量未测节点，避免无路可走
            pool = st.nodes.clone();
            pool.retain(|n| n.id != cur);
        }
        pool
    };
    if pool.is_empty() {
        say!("看护：无候选节点");
        return false;
    }
    sort_candidates_by_score(&mut pool);
    // 锚点：重拉失败会清空运行行，用它恢复映射再试下一个
    let anchor: Vec<crate::model::RunningProxy> = ctx
        .snapshot()
        .await
        .running
        .iter()
        .filter(|r| group.contains(&r.port))
        .cloned()
        .collect();
    for node in pool.into_iter().take(SWITCH_MAX_TRIES) {
        say!("看护尝试 [{}] {}:{} ..", node.sub, node.addr, node.port);
        if ctx.set_running_node(port, &node.id).await.is_err() {
            // 行已被上次失败清掉，先恢复锚点再试
            let _ = ctx.put_running(anchor.clone()).await;
            if ctx.set_running_node(port, &node.id).await.is_err() {
                continue;
            }
        }
        let snap = ctx.snapshot().await;
        stop_running_processes(&snap, &group).await;
        if relaunch_from_running(&ctx, &group).await.is_err() {
            say!("看护：启动失败，标死下一个");
            let _ = ctx.mark_dead(&node.id).await;
            let _ = ctx.put_running(anchor.clone()).await;
            continue;
        }
        let proxy = format!("socks5h://127.0.0.1:{port}");
        let ok = tester::http_get_via_socks(&proxy, &cfg.verify_url, timeout, tester::NO_BODY)
            .await
            .is_some_and(|(s, _, _)| verify_status_ok(s));
        if ok {
            say!("看护：已切换到 [{}] {}:{}", node.sub, node.addr, node.port);
            return true;
        }
        say!("看护：验证失败，标死下一个");
        let _ = ctx.mark_dead(&node.id).await;
    }
    say!("看护：候选耗尽仍未恢复，下周期重试");
    false
}
