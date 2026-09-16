use anyhow::{anyhow, Result};
use std::path::PathBuf;
use std::sync::Arc;

use crate::ctx::Ctx;
use crate::flow::{prune, sub_update, test};
use crate::model::{AppState, Node, WatchConfig};
use crate::probe::{fallback_candidates, streaming_probe_and_serve};
use crate::proxy::{launch_pairs, restart_running, run_single_with_failover};
use crate::rpc::{AutoParams, RunParams};
use crate::select::{
    describe_running, expand_pid_group, expand_stop_indices, node_matches, parse_ports, pick_next_node, rotate_node_ids, running_node_id, running_ports, select_switch_port, stop_target_ports, validate_strategy, validate_switch_selector,
};
use crate::watch::{start_watch, stop_watch, SWITCH_MAX_TRIES};
use crate::{run, store, tester};
use crate::say;

/// 锁住端口及其同进程组（空组回退自身）；guard 存活期间独占，函数结束自动释放
fn lock_group<'a>(
    ctx: &'a Ctx,
    st: &AppState,
    port: u16,
    owner: &str,
) -> Result<crate::ctx::PortGuard<'a>> {
    let g = expand_pid_group(&st.running, &[port]);
    let g = if g.is_empty() { vec![port] } else { g };
    ctx.acquire_ports(&g, owner)
}

/// 一键全流程：更新订阅 -> 测速 -> 去除失效 -> 流式探测上线 -> 常驻看护
pub async fn auto(ctx: &Arc<Ctx>, p: AutoParams) -> Result<()> {
    if p.port == 0 {
        return Err(anyhow!("端口必须在 1..=65535"));
    }
    // 同端口串行化：guard 存活到 auto 结束；内部接管走 stop_inner 直调（锁不可重入）
    let _port_guard = lock_group(ctx, &ctx.snapshot().await, p.port, &format!("auto#{}", p.port))?;
    // 探测参数以运行期唯一真源（内存 Settings）为缺省，CLI 指定则覆盖
    let settings = ctx.settings().await;
    let probe_url = p
        .probe_url
        .clone()
        .unwrap_or_else(|| settings.probe_url.clone());
    let probe_timeout = p.probe_timeout.unwrap_or(settings.probe_timeout);
    // 目标端口已在运行：先实测出口，可用才真正"无需操作"；死节点占端口则接管修复
    {
        let st = ctx.snapshot().await;
        if let Some(r) = st.running.iter().find(|r| r.port == p.port)
            && run::is_pid_alive(r.pid)
        {
            let timeout = settings.probe_timeout;
            let proxy = tester::socks_proxy_url(p.port);
            say!("端口 {} 已在运行，先实测 {} ...", p.port, probe_url);
            match tester::http_get_via_socks(&proxy, &probe_url, timeout, tester::NO_BODY).await {
                Some((s, bytes, ms)) if tester::is_reachable(s) => {
                    say!("  可用: {} {ms}ms {bytes}B，无需操作:", s);
                    say!("{}", describe_running(&st, r));
                    return Ok(());
                }
                other => {
                    let detail = other.map_or_else(|| "无响应".to_string(), |(s, _, _)| s.to_string());
                    say!("  不可用（{detail}），接管修复：停旧后走完整流程");
                    stop_inner(ctx, Some(p.port), false).await?;
                }
            }
        }
    }
    let subs_path = p
        .subs
        .clone().map_or_else(store::default_subs_path, PathBuf::from);
    // auto 链路的 prune 不删失败节点（drop_dead=false），所以没给 keep_top 时它无事可做，直接跳过
    let do_prune = !p.skip_prune && p.keep_top.is_some();
    let total = [!p.skip_update, !p.skip_test, do_prune, !p.skip_probe, true]
        .iter()
        .filter(|&&x| x)
        .count();
    let mut step = 0;
    if !p.skip_update {
        step += 1;
        say!("== [{step}/{total}] 更新订阅 ==");
        sub_update(ctx, &subs_path, p.name.clone()).await?;
    }
    if !p.skip_test {
        step += 1;
        say!("== [{step}/{total}] 测速 tcping ==");
        test(
            ctx,
            "tcping",
            p.test_concurrency,
            p.test_timeout,
            false,
            p.filter.clone(),
            1000,
        )
        .await?;
    }
    if do_prune {
        step += 1;
        say!("== [{step}/{total}] 裁剪到前 {} 名 ==", p.keep_top.unwrap());
        prune(ctx, 0, p.keep_top, false, false, false).await?;
    }
    if p.skip_probe {
        step += 1;
        say!("== [{step}/{total}] 启动代理 ==");
        run(
            ctx,
            RunParams {
                port: p.port,
                count: 1,
                ports: None,
                distinct_cc: false,
                strategy: "score".into(),
                daemon: !p.no_daemon,
                filter: p.filter.clone(),
                retries: p.retries,
                verify_url: Some(probe_url.clone()),
            },
        )
        .await?;
    } else {
        step += 1;
        say!("== [{step}/{total}] 真实探测（有可用即上线，快10%即替换） ==");
        if let Err(e) = streaming_probe_and_serve(ctx, &p).await {
            say!("流式探测未上线任何节点（{e:#}），回退 tcping 候选兜底");
            let ordered = fallback_candidates(ctx, &p.filter).await;
            run_single_with_failover(ctx, ordered, p.port, p.retries, &probe_url, probe_timeout).await?;
        }
    }
    if !p.no_daemon {
        let cfg = WatchConfig {
            filter: p.filter.clone(),
            verify_url: probe_url.clone(),
        };
        start_watch(ctx, p.port, cfg).await?;
        say!("进入常驻看护（改 config.toml 需 serve --stop 重启生效），失活自动更换");
    }
    Ok(())
}

/// 启动代理（单或多端口）；daemon 成功后带看护（失活自动更换）
pub async fn run(ctx: &Arc<Ctx>, p: RunParams) -> Result<()> {
    validate_strategy(&p.strategy)?;
    let st = ctx.snapshot().await;
    // 端口冲突检查：已跟踪且进程存活则拒绝
    let ports_vec: Vec<u16> = if let Some(s) = &p.ports {
        parse_ports(s)?
    } else {
        if p.count == 0 {
            return Err(anyhow!("count 必须大于 0"));
        }
        if p.port == 0 {
            return Err(anyhow!("端口必须在 1..=65535"));
        }
        let end = u64::from(p.port) + p.count as u64 - 1;
        if end > u64::from(u16::MAX) {
            return Err(anyhow!("端口范围超出 65535"));
        }
        (0..p.count).map(|i| p.port + i as u16).collect()
    };
    for pv in &ports_vec {
        if let Some(r) = st.running.iter().find(|r| &r.port == pv)
            && run::is_pid_alive(r.pid)
        {
            return Err(anyhow!(
                "端口 {pv} 已在运行 (pid={})，请先 stop --port {pv}",
                r.pid
            ));
        }
    }
    // 占锁防并发（guard 存活到 run 结束）
    let _port_guard = ctx.acquire_ports(
        &ports_vec,
        &format!("run#{}", ports_vec.first().copied().unwrap_or(0)),
    )?;
    let mut ordered: Vec<Node> = {
        let mut alive: Vec<Node> = st.nodes.iter().filter(|n| n.alive).cloned().collect();
        if let Some(f) = &p.filter {
            let re = regex::Regex::new(f).map_err(|e| anyhow!("filter regex {e}"))?;
            alive.retain(|n| node_matches(n, &re));
            if alive.is_empty() {
                return Err(anyhow!("过滤后无可用节点"));
            }
        }
        if alive.is_empty() {
            alive = st.nodes.clone();
            if alive.is_empty() {
                return Err(anyhow!("无节点"));
            }
        }
        // 默认 score：按 node_score（速度为主、延迟折算）降序；只有显式指定
        // least-latency 才按裸延迟排——延迟最低的节点常常吞吐很差
        if p.strategy == "least-latency" {
            crate::select::sort_nodes_by_delay(&mut alive);
        } else {
            crate::select::sort_candidates_by_score(&mut alive);
        }
        if p.distinct_cc {
            let mut seen = std::collections::HashSet::new();
            let mut distinct = Vec::new();
            let mut rest = Vec::new();
            for n in alive {
                let cc = n.cc.clone().unwrap_or_default();
                if cc.is_empty() || seen.insert(cc) {
                    distinct.push(n);
                } else {
                    rest.push(n);
                }
            }
            distinct.extend(rest);
            alive = distinct;
        }
        alive
    };
    if p.strategy == "random" {
        use rand::seq::SliceRandom;
        ordered.shuffle(&mut rand::rng());
    }
    // 验证目标：CLI 指定优先，否则取运行期配置
    let settings = ctx.settings().await;
    let verify_url = p
        .verify_url
        .clone()
        .unwrap_or_else(|| settings.probe_url.clone());
    let result = if p.daemon && ports_vec.len() == 1 {
        let timeout = settings.probe_timeout;
        run_single_with_failover(ctx, ordered, ports_vec[0], p.retries, &verify_url, timeout).await
    } else {
        // 多端口或前台：一次性启动（无顺延）
        let n = ports_vec.len();
        if ordered.len() < n {
            return Err(anyhow!("可用节点 {} 少于端口数 {}", ordered.len(), n));
        }
        let selected: Vec<Node> = ordered.into_iter().take(n).collect();
        launch_pairs(ctx, &selected, &ports_vec, p.daemon).await?;
        Ok(())
    };
    result?;
    if p.daemon {
        // 与 auto 一致：daemon 端口默认带看护（失活自动更换，参数写 meta 供 serve 重启恢复）
        for pv in &ports_vec {
            let cfg = WatchConfig {
                filter: p.filter.clone(),
                verify_url: verify_url.clone(),
            };
            start_watch(ctx, *pv, cfg).await?;
        }
        say!("进入常驻看护（改 config.toml 需 serve --stop 重启生效），失活自动更换");
    }
    Ok(())
}

/// 停止代理（--port 单停 / --all 全停），联动停看护
/// 入口先占目标端口锁再进核心；auto 内部已持锁，走 `stop_inner` 直调
pub async fn stop(ctx: &Ctx, port: Option<u16>, all: bool) -> Result<()> {
    let lock_ports = {
        let st = ctx.snapshot().await;
        stop_target_ports(&st.running, port, all).unwrap_or_default()
    };
    let _guard = if lock_ports.is_empty() {
        None
    } else {
        Some(ctx.acquire_ports(&lock_ports, "stop")?)
    };
    stop_inner(ctx, port, all).await
}

/// 锁-free 核心（调用方需已持有目标端口锁）
async fn stop_inner(ctx: &Ctx, port: Option<u16>, all: bool) -> Result<()> {
    let st = ctx.snapshot().await;
    if st.running.is_empty() {
        return Err(anyhow!(
            "无运行中代理（running 为空），如有残留进程请手动 pkill"
        ));
    }
    let ports_all: Vec<u16> = st.running.iter().map(|r| r.port).collect();
    let targets = run::select_stop_indices(&ports_all, port, all)?;
    let targets = expand_stop_indices(&st.running, &targets);
    // 先停看护，避免看护复活刚停的端口；同进程整组一起停
    let mut watch_ports: Vec<u16> = targets
        .iter()
        .filter_map(|&i| st.running.get(i).map(|r| r.port))
        .collect();
    watch_ports = expand_pid_group(&st.running, &watch_ports);
    for p in &watch_ports {
        stop_watch(ctx, *p).await?;
    }
    // 降序处理避免下标漂移
    let mut order = targets.clone();
    order.sort_unstable_by(|a, b| b.cmp(a));
    let mut handled_pids = std::collections::HashSet::new();
    let mut removed_configs = std::collections::HashSet::new();
    let mut target_ports: Vec<u16> = Vec::new();
    for idx in order {
        let r = st.running[idx].clone();
        target_ports.push(r.port);
        if r.pid != 0 && handled_pids.insert(r.pid) && run::is_pid_alive(r.pid) {
            say!("停止端口 {} pid={} ...", r.port, r.pid);
            let _ = run::kill_pid(r.pid, false);
            if !run::wait_for_pid_gone(r.pid, 3000).await {
                say!("  SIGTERM 超时，SIGKILL pid={}", r.pid);
                let _ = run::kill_pid(r.pid, true);
                let _ = run::wait_for_pid_gone(r.pid, 3000).await;
            }
            if !run::wait_for_ports_free(&[r.port], 1000).await {
                // pid 陈旧时按端口兜底回收，否则端口会一直被残留进程占着
                run::kill_port_listeners(&[r.port]).await;
                let _ = run::wait_for_ports_free(&[r.port], 3000).await;
            }
            say!("  端口 {} 已停止", r.port);
        } else if r.pid == 0 || !handled_pids.contains(&r.pid) {
            say!("端口 {} pid={} 已不在运行，仅清理状态", r.port, r.pid);
        }
        // 删除配置文件，保留日志备查
        if !r.config_path.is_empty() && removed_configs.insert(r.config_path.clone()) {
            let _ = std::fs::remove_file(&r.config_path);
        }
    }
    ctx.remove_running(&target_ports).await?;
    say!(
        "stop 完成，剩余运行 {} 个",
        ctx.snapshot().await.running.len()
    );
    Ok(())
}

/// 切换节点（默认热切换，自动重启 sing-box 立即生效；假活节点当场删除）
pub async fn switch_cmd(
    ctx: &Ctx,
    which: String,
    port: Option<u16>,
    all: bool,
    no_restart: bool,
) -> Result<()> {
    validate_switch_selector(&which)?;
    let st = ctx.snapshot().await;
    if st.running.is_empty() {
        return Err(anyhow!("无运行中代理，请先 run"));
    }
    // 候选池：probe 验证过（有出口 IP）的节点优先，其余在后，各组内按延迟排序
    let mut alive: Vec<Node> = st.nodes.iter().filter(|n| n.alive).cloned().collect();
    if alive.is_empty() {
        alive = st.nodes.clone();
    }
    if alive.is_empty() {
        return Err(anyhow!("无可用节点"));
    }
    crate::select::sort_nodes_by_delay(&mut alive);
    alive.sort_by_key(|n| std::cmp::Reverse(n.exit_ip.is_some()));

    if all {
        let ports = running_ports(&st);
        let _guard = ctx.acquire_ports(&ports, "switch --all")?;
        let cur_ids: Vec<String> = ports.iter().map(|p| running_node_id(&st, *p)).collect();
        let next_ids = rotate_node_ids(&alive, &cur_ids, ports.len());
        for (p, id) in ports.iter().zip(next_ids) {
            ctx.set_running_node(*p, &id).await?;
        }
        let st2 = ctx.snapshot().await;
        say!("已整体轮换，新的映射:");
        for p in &ports {
            say!("  {p} -> {}", running_node_id(&st2, *p));
        }
        if no_restart {
            say!(
                "已仅更新映射（--no-restart），需重启生效: proxytool run --ports {}",
                ports
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            );
            return Ok(());
        }
        let ports = running_ports(&ctx.snapshot().await);
        say!("重启 sing-box 使切换生效（所有端口短暂中断）...");
        restart_running(ctx, &ports).await?;
        say!("热切换完成（--all 不做逐节点验证；单端口请用 --port <port>）");
        return Ok(());
    }

    let ports = running_ports(&st);
    let p = select_switch_port(&ports, port)?;
    if !st.running.iter().any(|r| r.port == p) {
        return Err(anyhow!("端口 {p} 不在运行中"));
    }
    // 同进程组一起锁（restart 会整组停起）
    let _guard = lock_group(ctx, &st, p, &format!("switch#{p}"))?;
    if no_restart {
        let next = pick_next_node(&which, &alive, &running_node_id(&st, p));
        ctx.set_running_node(p, &next.id).await?;
        say!(
            "端口 {p} 已切换 -> [{}] {}:{}（--no-restart：需重启生效）",
            next.sub, next.addr, next.port
        );
        return Ok(());
    }

    // 单端口自动验证循环：切换后实测 probe_url，不通自动顺延；假活节点标死不删
    // （单次探测失败可能是限流/抖动，删掉就再也没机会复活，只有 prune --drop-dead 才删）
    let settings = ctx.settings().await;
    let probe_url = settings.probe_url.clone();
    let ip_url = settings.ip_api_url.clone();
    let timeout = settings.probe_timeout;
    let proxy = tester::socks_proxy_url(p);
    let mut marked = 0usize;
    let mut attempt = 0usize;
    let started = std::time::Instant::now();
    loop {
        if attempt >= SWITCH_MAX_TRIES {
            return Err(anyhow!(
                "连续 {attempt} 个节点均无法访问 {probe_url}（已标死假活节点 {marked} 个）；可更新订阅或 probe 后重试"
            ));
        }
        attempt += 1;
        let cur = running_node_id(&ctx.snapshot().await, p);
        let next = pick_next_node(&which, &alive, &cur);
        ctx.set_running_node(p, &next.id).await?;
        say!(
            "[{attempt}/{SWITCH_MAX_TRIES}] 切换 {p} -> [{}] {}:{}",
            next.sub, next.addr, next.port
        );
        if let Err(e) = restart_running(ctx, &[p]).await {
            say!("  启动失败: {e}（已标死该节点）");
            ctx.mark_dead(&next.id).await?;
            alive.retain(|n| n.id != next.id);
            marked += 1;
            continue;
        }
        let speed =
            tester::http_get_via_socks(&proxy, &probe_url, timeout, tester::SPEED_SAMPLE_BYTES).await;
        if speed.as_ref().is_some_and(|(s, _, _)| tester::is_reachable(*s)) {
            let (s, bytes, ms) = speed.unwrap();
            let (ip, cc) =
                crate::ipinfo::fetch_ip_via_proxy(&proxy, &ip_url, tester::IPINFO_TIMEOUT_SECS)
                    .await
                    .unwrap_or(("-".into(), String::new()));
            say!(
                "验证通过: HTTP {s} {ms}ms {bytes}B 出口={ip} {cc}（总耗时 {}s）",
                started.elapsed().as_secs()
            );
            return Ok(());
        }
        // speed 不通 → 探出口区分「节点假活」与「目标站拒绝该出口」
        let ip_probe = tester::http_get_via_socks(&proxy, &ip_url, 8, tester::NO_BODY).await;
        if ip_probe.as_ref().is_some_and(|(s, _, _)| tester::is_reachable(*s)) {
            say!("  节点可用但无法访问 {probe_url}（跳过，不删除）");
        } else {
            say!("  节点假活（出口也不通），已标死");
            ctx.mark_dead(&next.id).await?;
            alive.retain(|n| n.id != next.id);
            marked += 1;
        }
    }
}
