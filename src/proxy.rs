use anyhow::{anyhow, Result};
use std::collections::HashSet;

use crate::ctx::Ctx;
use crate::model::{AppState, Node, RunningProxy};
use crate::run::Launched;
use crate::select::resolve_mapped_nodes;
use crate::{config_gen, run, tester};
use crate::say;

/// 单次启动 pairs 并落盘运行态；端口未就绪则 kill 清理后返回 Err（调用方决定是否顺延）
pub async fn launch_pairs(
    ctx: &Ctx,
    selected: &[Node],
    ports_vec: &[u16],
    daemon: bool,
) -> Result<Launched> {
    let pairs: Vec<(&Node, u16)> = selected
        .iter()
        .zip(ports_vec.iter())
        .map(|(a, b)| (a, *b))
        .collect();
    let listen = ctx.snapshot().await.settings.listen_addr;
    say!("启动 {} 个代理:", pairs.len());
    for (node, p) in &pairs {
        say!(
            "  {listen}:{p} -> [{}] {}:{} {}ms {:.1}KB/s",
            node.sub,
            node.addr,
            node.port,
            node.delay_ms,
            node.speed_kbps.unwrap_or(0.0)
        );
    }

    // 端口被非托管进程占用时必须先报错：否则就绪探测会把它当成"自己起来了"，
    // 实际 sing-box 根本没起来，后续验证失败会误判节点假活并连锁标死
    if !run::are_ports_free(ports_vec).await {
        return Err(anyhow!(
            "端口 {} 已被占用（可能是未托管的残留进程），请先释放后重试",
            ports_vec
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }

    let cfg = config_gen::generate_singbox_config(&pairs, &listen)?;
    let cfg_path = run::generate_config_path();
    std::fs::write(&cfg_path, serde_json::to_string_pretty(&cfg)?)?;
    say!("已生成 {}", cfg_path.display());

    let Some(bin) = crate::tester::singbox_bin() else {
        say!(
            "未找到 sing-box，仅生成配置，请手动安装后运行: sing-box run -c {}",
            cfg_path.display()
        );
        // pid=0 表示无实际进程，调用方不得做存活验证
        return Ok(Launched {
            pid: 0,
            config_path: cfg_path,
        });
    };
    say!("检测到 {}，尝试启动...", bin.display());
    let log_path = cfg_path.with_extension("log");
    let log_file = std::fs::File::create(&log_path)?;
    let log_err = log_file.try_clone()?;
    let mut cmd = tokio::process::Command::new(&bin);
    cmd.args(["run", "-c", &cfg_path.to_string_lossy()])
        .stdout(std::process::Stdio::from(log_file))
        .stderr(std::process::Stdio::from(log_err));
    // 始终脱离进程组：server/CLI 退出时 sing-box 不被连坐（server 重启可 re-adopt）
    cmd.process_group(0);
    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(error) => {
            let _ = std::fs::remove_file(&cfg_path);
            return Err(error.into());
        }
    };
    let pid = child.id().ok_or_else(|| anyhow!("无法获取 sing-box PID"))?;
    say!(
        "已启动 pid={pid}，等待端口就绪... 日志: {}",
        log_path.display()
    );
    if !run::wait_for_ports(ports_vec, 5000).await {
        say!("端口未就绪，请检查日志:\n{}", run::tail_file(&log_path, 30));
        let _ = child.kill().await;
        let _ = child.wait().await;
        let _ = std::fs::remove_file(&cfg_path);
        return Err(anyhow!(
            "端口未就绪（{}），已终止本次启动",
            log_path.display()
        ));
    }
    say!("全部端口就绪");
    for p in ports_vec {
        say!("  curl -x socks5h://127.0.0.1:{p} https://api.ip.sb/geoip");
    }
    if listen != "127.0.0.1" {
        say!("监听 {listen}：内网其他机器可用 <本机IP>:<端口> 直连（无认证，注意暴露面）");
    }
    let now = chrono::Utc::now();
    let entries: Vec<RunningProxy> = selected
        .iter()
        .zip(ports_vec.iter())
        .map(|(node, p)| RunningProxy {
            port: *p,
            node_id: node.id.clone(),
            pid,
            config_path: cfg_path.to_string_lossy().to_string(),
            log_path: log_path.to_string_lossy().to_string(),
            started_at: Some(now),
        })
        .collect();
    if let Err(e) = ctx.put_running(entries).await {
        let _ = child.kill().await;
        let _ = child.wait().await;
        let _ = std::fs::remove_file(&cfg_path);
        return Err(e);
    }
    run::prune_old_files(
        &crate::store::singbox_dir(),
        20,
        "singbox-",
        &[cfg_path.clone(), log_path.clone()],
    );
    if !daemon {
        say!("前台运行中，等待退出...");
        let ports = ports_vec.to_vec();
        let _ = child.wait().await;
        // 前台退出后清理运行态（进程已结束）
        let _ = ctx.remove_running(&ports).await;
    } else {
        say!("daemon 模式 pid={pid} 已分离");
    }
    Ok(Launched {
        pid,
        config_path: cfg_path,
    })
}

/// 清理一次失败尝试：杀进程、删配置留日志、清运行态
pub async fn discard_attempt(ctx: &Ctx, port: u16, launched: &Launched) {
    if launched.pid != 0 {
        let _ = run::kill_pid(launched.pid, false);
        run::wait_for_pid_gone(launched.pid, 3000).await;
    }
    let _ = std::fs::remove_file(&launched.config_path);
    let _ = ctx.remove_running(&[port]).await;
}

/// 杀掉 running 表中指定端口的存活进程（多端口共享同一 pid，去重），并等待端口释放
pub async fn stop_running_processes(st: &AppState, ports: &[u16]) {
    let mut handled = HashSet::new();
    for r in &st.running {
        if r.pid != 0
            && ports.contains(&r.port)
            && handled.insert(r.pid)
            && run::is_pid_alive(r.pid)
        {
            say!("停止旧进程 pid={} ...", r.pid);
            let _ = run::kill_pid(r.pid, false);
            if !run::wait_for_pid_gone(r.pid, 3000).await {
                let _ = run::kill_pid(r.pid, true);
                let _ = run::wait_for_pid_gone(r.pid, 3000).await;
            }
        }
    }
    let _ = run::wait_for_ports_free(ports, 3000).await;
}

/// 按运行态映射重新拉起 sing-box：解析节点 -> launch_pairs 重写运行态
/// 启动前不清运行行：成功由 put_running 覆盖，失败则保留原映射
/// （先清再起会在启动失败时让运行态凭空消失，看护与 switch 都再也找不回映射）
pub async fn relaunch_from_running(ctx: &Ctx, ports: &[u16]) -> Result<()> {
    let selected = {
        let st = ctx.state.read().await;
        resolve_mapped_nodes(&st, ports)?
    };
    launch_pairs(ctx, &selected, ports, true).await.map(|_| ())
}

/// 热切换核心：杀旧进程，按切换后的映射重启；新节点起不来则回退旧节点
/// 同进程整组一起停起（多端口共享 pid 时只动一个会杀掉整组进程）
pub async fn restart_running(ctx: &Ctx, ports: &[u16]) -> Result<()> {
    let group: Vec<u16> = {
        let st = ctx.snapshot().await;
        let g = crate::select::expand_pid_group(&st.running, ports);
        if g.is_empty() { ports.to_vec() } else { g }
    };
    let ports = group.as_slice();
    let old_entries: Vec<RunningProxy> = {
        let st = ctx.state.read().await;
        st.running
            .iter()
            .filter(|r| ports.contains(&r.port))
            .cloned()
            .collect()
    };
    let snap = ctx.snapshot().await;
    stop_running_processes(&snap, ports).await;
    if let Err(e) = relaunch_from_running(ctx, ports).await {
        say!("新节点启动失败，回退旧节点: {e}");
        // 恢复旧映射（pid 已死，仅取 node_id 用）
        let _ = ctx.put_running(old_entries).await;
        let snap = ctx.snapshot().await;
        stop_running_processes(&snap, ports).await;
        relaunch_from_running(ctx, ports).await.map_err(|e2| {
            anyhow!(
                "回退旧节点也失败，请手动运行: proxytool run --ports {}: {e2}",
                ports
                    .iter()
                    .map(|p| p.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            )
        })?;
        return Err(anyhow!("已回退旧节点，本次切换未生效"));
    }
    Ok(())
}

/// 单端口 daemon：按候选顺序逐个尝试，启动+验证失败自动顺延下一个
pub async fn run_single_with_failover(
    ctx: &Ctx,
    ordered: Vec<Node>,
    port: u16,
    retries: usize,
    verify_url: &str,
    timeout: u64,
) -> Result<()> {
    let timeout = timeout.max(1);
    let ports_vec = vec![port];
    let proxy = format!("socks5h://127.0.0.1:{port}");
    let mut queue: std::collections::VecDeque<Node> = ordered.into_iter().collect();
    let max_tries = retries.max(1);
    let mut attempts = 0;
    loop {
        let Some(node) = queue.pop_front() else {
            return Err(anyhow!("候选节点耗尽，代理启动失败"));
        };
        attempts += 1;
        if attempts > max_tries {
            return Err(anyhow!("已尝试 {max_tries} 个候选，均失败"));
        }
        say!(
            "尝试 {attempts}/{max_tries}: [{}] {}:{} {}ms",
            node.sub, node.addr, node.port, node.delay_ms
        );
        let launched = match launch_pairs(ctx, std::slice::from_ref(&node), &ports_vec, true).await {
            Ok(l) => l,
            Err(e) => {
                say!("  启动失败: {e}，下一个");
                continue;
            }
        };
        if launched.pid == 0 {
            return Err(anyhow!("未找到 sing-box，无法验证，已生成配置"));
        }
        match tester::http_get_via_socks(&proxy, verify_url, timeout, tester::NO_BODY).await {
            Some((status, bytes, ms)) if tester::is_reachable(status) => {
                say!("验证通过: {status} {ms}ms {bytes}B，代理就绪");
                return Ok(());
            }
            other => {
                let detail = other
                    .map(|(s, _, _)| s.to_string())
                    .unwrap_or_else(|| "无响应".to_string());
                say!("  验证失败 ({detail})，标死该节点，下一个");
                let _ = ctx.mark_dead(&node.id).await;
                discard_attempt(ctx, port, &launched).await;
            }
        }
    }
}

/// 单节点全新上线并验证；验证失败标死清理返回 false
pub async fn launch_fresh(
    ctx: &Ctx,
    node: &Node,
    port: u16,
    verify_url: &str,
    timeout: u64,
) -> Result<bool> {
    let timeout = timeout.max(1);
    let launched = match launch_pairs(ctx, std::slice::from_ref(node), &[port], true).await {
        Ok(l) => l,
        Err(e) => {
            say!("   上线失败: {e}");
            return Ok(false);
        }
    };
    if launched.pid == 0 {
        return Err(anyhow!("未找到 sing-box，无法验证，已生成配置"));
    }
    let proxy = format!("socks5h://127.0.0.1:{port}");
    match tester::http_get_via_socks(&proxy, verify_url, timeout, tester::NO_BODY).await {
        Some((status, bytes, ms)) if tester::is_reachable(status) => {
            say!("   验证通过: {status} {ms}ms {bytes}B");
            Ok(true)
        }
        other => {
            let detail = other
                .map(|(s, _, _)| s.to_string())
                .unwrap_or_else(|| "无响应".to_string());
            say!("   验证失败 ({detail})，标死该节点");
            let _ = ctx.mark_dead(&node.id).await;
            discard_attempt(ctx, port, &launched).await;
            Ok(false)
        }
    }
}

/// 在役替换：停旧起新并验证；失败回退旧节点返回 false
/// 同进程整组一起停起；重拉失败会清空运行行，用锚点恢复映射后再回退
pub async fn replace_live(
    ctx: &Ctx,
    port: u16,
    new_id: &str,
    verify_url: &str,
    timeout: u64,
) -> Result<bool> {
    let timeout = timeout.max(1);
    // 锚点：目标映射 + 同组运行行（回滚用）
    let (prev_id, anchor, group) = {
        let st = ctx.snapshot().await;
        let Some(entry) = st.running.iter().find(|r| r.port == port).cloned() else {
            say!("   端口运行态已消失，取消替换");
            return Ok(false);
        };
        let g = crate::select::expand_pid_group(&st.running, &[port]);
        let g = if g.is_empty() { vec![port] } else { g };
        let rows: Vec<RunningProxy> = st
            .running
            .iter()
            .filter(|r| g.contains(&r.port))
            .cloned()
            .collect();
        (entry.node_id.clone(), rows, g)
    };
    ctx.set_running_node(port, new_id).await?;
    let snap = ctx.snapshot().await;
    stop_running_processes(&snap, &group).await;
    if let Err(e) = relaunch_from_running(ctx, &group).await {
        say!("   新节点启动失败: {e}，回退旧节点");
        let _ = ctx.put_running(anchor.clone()).await;
        let _ = ctx.set_running_node(port, &prev_id).await;
        let snap = ctx.snapshot().await;
        stop_running_processes(&snap, &group).await;
        let _ = relaunch_from_running(ctx, &group).await;
        let _ = ctx.mark_dead(new_id).await;
        return Ok(false);
    }
    let proxy = format!("socks5h://127.0.0.1:{port}");
    match tester::http_get_via_socks(&proxy, verify_url, timeout, tester::NO_BODY).await {
        Some((status, bytes, ms)) if tester::is_reachable(status) => {
            say!("   替换验证通过: {status} {ms}ms {bytes}B");
            Ok(true)
        }
        other => {
            let detail = other
                .map(|(s, _, _)| s.to_string())
                .unwrap_or_else(|| "无响应".to_string());
            say!("   替换验证失败 ({detail})，回退旧节点");
            let _ = ctx.mark_dead(new_id).await;
            let _ = ctx.put_running(anchor.clone()).await;
            let _ = ctx.set_running_node(port, &prev_id).await;
            let snap = ctx.snapshot().await;
            stop_running_processes(&snap, &group).await;
            let _ = relaunch_from_running(ctx, &group).await;
            Ok(false)
        }
    }
}
