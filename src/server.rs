use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::watch as tokio_watch;

use crate::cmd;
use crate::ctx::{self, Ctx};
use crate::flow;
use crate::model::WatchConfig;
use crate::probe;
use crate::rpc::{self, JobIdArgs, Req, Resp, WatchArgs, WatchStopArgs};
use crate::store;
use crate::watch;

/// proxytool serve 入口：默认后台分叉（立即返回），--foreground 前台调试，--stop 停止
pub async fn serve(foreground: bool, stop: bool, _subs: Option<String>) -> Result<()> {
    if stop {
        return stop_server().await;
    }
    if foreground {
        return run_foreground().await;
    }
    spawn_detached()
}

/// server.stop 直连（不自动拉起）
async fn stop_server() -> Result<()> {
    match client_call("server.stop", serde_json::Value::Null).await {
        Ok(r) if r.ok => {
            println!("server 已停止");
            Ok(())
        }
        Ok(r) => Err(anyhow!(r.error.unwrap_or_else(|| "server.stop 失败".into()))),
        Err(_) => Err(anyhow!("server 未运行（socket 不存在或无响应）")),
    }
}

async fn client_call(cmd: &str, args: serde_json::Value) -> Result<Resp> {
    crate::client::call_direct(cmd, args).await
}

/// 后台分叉 serve：独立进程组，输出进 serve-*.log（serve 默认路径与 CLI 自动拉起共用）
pub fn spawn_detached() -> Result<()> {
    use std::os::unix::process::CommandExt;
    let exe = std::env::current_exe()?;
    let dir = store::data_dir();
    std::fs::create_dir_all(&dir).ok();
    let log = dir.join(format!("serve-{}.log", chrono::Utc::now().timestamp_millis()));
    let f = std::fs::OpenOptions::new().create(true).append(true).open(&log)?;
    let fe = f.try_clone()?;
    let child = std::process::Command::new(exe)
        .args(["serve", "--foreground"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::from(f))
        .stderr(std::process::Stdio::from(fe))
        .process_group(0)
        .spawn()
        .map_err(|e| anyhow!("后台启动 serve 失败: {e}"))?;
    // serve 日志只留最近 3 组，防常驻无限 append
    crate::run::prune_old_files(&dir, 3, "serve-", std::slice::from_ref(&log));
    println!(
        "serve 已后台启动 pid={} 日志={}（tail -f 跟踪）",
        child.id(),
        log.display()
    );
    Ok(())
}

async fn bind_socket() -> Result<UnixListener> {
    let path = store::socket_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if path.exists() {
        if UnixStream::connect(&path).await.is_ok() {
            return Err(anyhow!("server 已在运行（socket {}）", path.display()));
        }
        // socket 残留但无响应：清掉重建
        let _ = std::fs::remove_file(&path);
    }
    Ok(UnixListener::bind(&path)?)
}

async fn run_foreground() -> Result<()> {
    let (shutdown_tx, mut shutdown_rx) = tokio_watch::channel(false);
    let listener = bind_socket().await?;
    // 先由主线程完成 DB 初始化（journal_mode/schema），writer 线程再开连接，避免并发 PRAGMA 竞争
    let mut st = store::load_state()?;
    st.settings = crate::config::load_or_create()?;
    let db = crate::db::spawn_writer()?;
    let ctx = Arc::new(Ctx {
        state: Arc::new(tokio::sync::RwLock::new(st)),
        db,
        jobs: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        job_seq: std::sync::atomic::AtomicU64::new(0),
        watches: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        port_locks: ctx::PortLocks::default(),
        write_mu: tokio::sync::Mutex::new(()),
    });
    // re-adopt：running 存活条目按 meta watch 配置恢复看护
    watch::adopt_running(&ctx).await;
    ctx.set_meta("server.pid", Some(std::process::id().to_string()))
        .await?;
    println!(
        "serve 已就绪 socket={} pid={}（所有写命令经此单写者执行）",
        store::socket_path().display(),
        std::process::id()
    );
    let ctx_acc = ctx.clone();
    let tx_acc = shutdown_tx.clone();
    let accept_loop = async move {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let c = ctx_acc.clone();
                    let tx = tx_acc.clone();
                    tokio::spawn(handle_conn(c, tx, stream));
                }
                Err(e) => eprintln!("accept 失败: {e}"),
            }
        }
    };
    tokio::select! {
        _ = accept_loop => {}
        _ = tokio::signal::ctrl_c() => {}
        _ = shutdown_rx.changed() => {}
    }
    teardown(ctx).await;
    Ok(())
}

/// 响应之后执行的副作用
#[derive(PartialEq)]
enum Action {
    Shutdown,
}

async fn handle_conn(ctx: Arc<Ctx>, shutdown: tokio_watch::Sender<bool>, stream: UnixStream) {
    let (rd, mut wr) = stream.into_split();
    let mut lines = BufReader::new(rd).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let (resp, action) = match serde_json::from_str::<Req>(&line) {
            Ok(req) => dispatch(&ctx, &req).await,
            Err(e) => (Resp::err(format!("请求解析失败: {e}")), None),
        };
        let mut out = serde_json::to_string(&resp).unwrap_or_else(|_| {
            r#"{"ok":false,"error":"resp 序列化失败"}"#.to_string()
        });
        out.push('\n');
        if wr.write_all(out.as_bytes()).await.is_err() {
            break;
        }
        if action == Some(Action::Shutdown) {
            let _ = shutdown.send(true);
            break;
        }
    }
}

async fn dispatch(ctx: &Arc<Ctx>, req: &Req) -> (Resp, Option<Action>) {
    match req.cmd.as_str() {
        "ping" => (
            Resp::ok(serde_json::json!({"pong": true})),
            None,
        ),
        "server.stop" => (Resp::ok(serde_json::Value::Null), Some(Action::Shutdown)),
        "job.status" => {
            let Ok(a) = serde_json::from_value::<JobIdArgs>(req.args.clone()) else {
                return (Resp::err("job_id 缺失"), None);
            };
            let jobs = ctx.jobs.lock().await;
            match jobs.get(&a.job_id) {
                Some(j) => (
                    Resp::ok(serde_json::json!({
                        "running": j.running,
                        "ok": j.ok,
                        "error": j.error,
                        "log": j.log,
                    })),
                    None,
                ),
                None => (Resp::err(format!("job {} 不存在", a.job_id)), None),
            }
        }
        "watch.start" => {
            let Ok(a) = serde_json::from_value::<WatchArgs>(req.args.clone()) else {
                return (Resp::err("watch 参数缺失"), None);
            };
            let cfg = WatchConfig {
                filter: a.filter,
                verify_url: a
                    .probe_url
                    .unwrap_or_else(|| "https://www.google.com/".into()),
            };
            match watch::start_watch(ctx, a.port, cfg).await {
                Ok(()) => (Resp::ok(serde_json::Value::Null), None),
                Err(e) => (Resp::err(format!("{e:#}")), None),
            }
        }
        "watch.stop" => {
            let Ok(a) = serde_json::from_value::<WatchStopArgs>(req.args.clone()) else {
                return (Resp::err("port 缺失"), None);
            };
            match watch::stop_watch(ctx, a.port).await {
                Ok(()) => (Resp::ok(serde_json::Value::Null), None),
                Err(e) => (Resp::err(format!("{e:#}")), None),
            }
        }
        "sub.update" => {
            let Ok(p) = serde_json::from_value::<rpc::SubUpdateParams>(req.args.clone()) else {
                return (Resp::err("参数解析失败"), None);
            };
            let path = p
                .subs
                .clone()
                .map(std::path::PathBuf::from)
                .unwrap_or_else(store::default_subs_path);
            let c = ctx.clone();
            job_resp(
                ctx.clone(),
                "sub.update",
                async move { flow::sub_update(&c, &path, p.name).await },
            )
            .await
        }
        "test" => {
            let Ok(p) = serde_json::from_value::<rpc::TestParams>(req.args.clone()) else {
                return (Resp::err("参数解析失败"), None);
            };
            let c = ctx.clone();
            job_resp(
                ctx.clone(),
                "test",
                async move {
                    flow::test(&c, &p.mode, p.concurrency, p.timeout, p.with_ipinfo, p.filter, p.top)
                        .await
                },
            )
            .await
        }
        "prune" => {
            let Ok(p) = serde_json::from_value::<rpc::PruneParams>(req.args.clone()) else {
                return (Resp::err("参数解析失败"), None);
            };
            let c = ctx.clone();
            job_resp(
                ctx.clone(),
                "prune",
                async move {
                    flow::prune(&c, p.delay_threshold, p.keep_top, p.dedup_endpoint, p.invalid).await
                },
            )
            .await
        }
        "probe" => {
            let Ok(p) = serde_json::from_value::<rpc::ProbeParams>(req.args.clone()) else {
                return (Resp::err("参数解析失败"), None);
            };
            let c = ctx.clone();
            job_resp(
                ctx.clone(),
                "probe",
                async move {
                    probe::probe(&c, p.batch_size, p.timeout, &p.probe_url, p.max_batches, p.concurrency, p.filter)
                        .await
                },
            )
            .await
        }
        "auto" => {
            let Ok(p) = serde_json::from_value::<rpc::AutoParams>(req.args.clone()) else {
                return (Resp::err("参数解析失败"), None);
            };
            let c = ctx.clone();
            job_resp(ctx.clone(), "auto", async move { cmd::auto(&c, p).await }).await
        }
        "run" => {
            let Ok(p) = serde_json::from_value::<rpc::RunParams>(req.args.clone()) else {
                return (Resp::err("参数解析失败"), None);
            };
            let c = ctx.clone();
            job_resp(ctx.clone(), "run", async move { cmd::run(&c, p).await }).await
        }
        "stop" => {
            let Ok(p) = serde_json::from_value::<rpc::StopParams>(req.args.clone()) else {
                return (Resp::err("参数解析失败"), None);
            };
            let c = ctx.clone();
            job_resp(
                ctx.clone(),
                "stop",
                async move { cmd::stop(&c, p.port, p.all).await },
            )
            .await
        }
        "switch" => {
            let Ok(p) = serde_json::from_value::<rpc::SwitchParams>(req.args.clone()) else {
                return (Resp::err("参数解析失败"), None);
            };
            let c = ctx.clone();
            job_resp(
                ctx.clone(),
                "switch",
                async move { cmd::switch_cmd(&c, p.which, p.port, p.all, p.no_restart).await },
            )
            .await
        }
        "ipinfo" => {
            let Ok(p) = serde_json::from_value::<rpc::IpinfoParams>(req.args.clone()) else {
                return (Resp::err("参数解析失败"), None);
            };
            let c = ctx.clone();
            job_resp(ctx.clone(), "ipinfo", async move { flow::ipinfo(&c, p.concurrency).await }).await
        }
        other => (
            Resp::err(format!("未知命令: {other}（协议 v{}）", req.v)),
            None,
        ),
    }
}

async fn job_resp<F>(ctx: Arc<Ctx>, kind: &str, fut: F) -> (Resp, Option<Action>)
where
    F: std::future::Future<Output = Result<()>> + Send + 'static,
{
    match ctx::spawn_job(ctx, kind, fut).await {
        Ok((id, log)) => (Resp::ok(serde_json::json!({ "job_id": id, "log": log })), None),
        Err(e) => (Resp::err(format!("{e:#}")), None),
    }
}

/// 退出收尾：停看护与 job，杀全部托管代理，清 socket 与 meta
async fn teardown(ctx: Arc<Ctx>) {
    println!("serve 正在退出...");
    for (port, h) in ctx.watches.lock().await.drain() {
        h.abort();
        let _ = ctx.set_meta(&crate::model::watch_key(port), None).await;
        let _ = ctx.set_meta(&crate::model::watch_status_key(port), None).await;
    }
    for j in ctx.jobs.lock().await.values_mut() {
        if j.running
            && let Some(h) = j.handle.take()
        {
            h.abort();
        }
    }
    let st = ctx.snapshot().await;
    let ports: Vec<u16> = st.running.iter().map(|r| r.port).collect();
    for r in &st.running {
        if r.pid != 0 && crate::run::is_pid_alive(r.pid) {
            let _ = crate::run::kill_pid(r.pid, false);
        }
    }
    if !ports.is_empty() {
        let _ = crate::run::wait_for_ports_free(&ports, 3000).await;
    }
    let _ = ctx.remove_running(&ports).await;
    let _ = ctx.set_meta("server.pid", None).await;
    let _ = std::fs::remove_file(store::socket_path());
    println!("serve 已退出");
}
