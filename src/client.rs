use anyhow::{anyhow, Result};
use serde_json::Value;
use std::io::Write as IoWrite;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use crate::rpc::{Req, Resp};
use crate::store;

async fn connect_ok() -> bool {
    UnixStream::connect(store::socket_path()).await.is_ok()
}

async fn try_call(cmd: &str, args: serde_json::Value) -> Result<Resp> {
    let stream = UnixStream::connect(store::socket_path())
        .await
        .map_err(|e| anyhow!("连接 server 失败: {e}"))?;
    let req = Req {
        v: 1,
        cmd: cmd.to_string(),
        args,
    };
    let mut line = serde_json::to_string(&req)?;
    line.push('\n');
    let (rd, mut wr) = stream.into_split();
    wr.write_all(line.as_bytes()).await?;
    wr.flush().await?;
    let mut reader = BufReader::new(rd);
    let mut buf = String::new();
    tokio::time::timeout(std::time::Duration::from_secs(30), reader.read_line(&mut buf))
        .await
        .map_err(|_| anyhow!("server 响应超时"))?
        .map_err(|e| anyhow!("读 server 响应失败: {e}"))?;
    serde_json::from_str::<Resp>(buf.trim()).map_err(|e| anyhow!("响应解析失败: {e}"))
}

/// 写命令入口：连不上 server 才自动后台拉起后重试一次
/// （响应超时说明 server 活着但忙，拉起新实例也无用，直接报错）
pub async fn call(cmd: &str, args: serde_json::Value) -> Result<Resp> {
    match try_call(cmd, args.clone()).await {
        Ok(r) => Ok(r),
        Err(e) => {
            if connect_ok().await {
                return Err(e);
            }
            ensure_server()
                .await
                .map_err(|e2| anyhow!("server 不可用且自动拉起失败: {e2}（原始错误: {e}）"))?;
            try_call(cmd, args).await
        }
    }
}

/// 直连（不自动拉起）：server.stop 等管理命令用
pub async fn call_direct(cmd: &str, args: serde_json::Value) -> Result<Resp> {
    try_call(cmd, args).await
}

async fn ensure_server() -> Result<()> {
    if connect_ok().await {
        return Ok(());
    }
    // 与 serve 默认路径共用同一分叉逻辑（后台、独立进程组）
    crate::server::spawn_detached()?;
    for _ in 0..50 {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        if connect_ok().await {
            return Ok(());
        }
    }
    Err(anyhow!("server 启动超时"))
}

/// 增量读取日志文件新内容打印到 stdout（异步版，不占 executor 线程）
async fn drain_log(log: &std::path::Path, off: &mut u64) {
    let Ok(meta) = tokio::fs::metadata(log).await else {
        return;
    };
    if meta.len() <= *off {
        return;
    }
    let Ok(mut f) = tokio::fs::File::open(log).await else {
        return;
    };
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    if f.seek(std::io::SeekFrom::Start(*off)).await.is_err() {
        return;
    }
    let mut buf = Vec::new();
    let n = f.read_to_end(&mut buf).await.unwrap_or(0);
    *off += n as u64;
    if !buf.is_empty() {
        std::io::stdout().write_all(&buf).ok();
        std::io::stdout().flush().ok();
    }
}

/// 提交 job：follow=true 时实时回显 job 日志直至完成（Ctrl+C 只退出回显，job 继续）
pub async fn run_job(kind: &str, args: serde_json::Value, follow: bool) -> Result<()> {
    let resp = call(kind, args).await?;
    if !resp.ok {
        return Err(anyhow!(resp.error.unwrap_or_else(|| "job 提交被拒绝".into())));
    }
    let data = resp
        .data
        .ok_or_else(|| anyhow!("job 提交无数据"))?;
    let job_id = data
        .get("job_id")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("job_id 缺失"))?;
    let log: String = data
        .get("log")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    if !follow {
        println!("已提交 job {kind}#{job_id} 日志={log}（后台执行，tail -f 跟踪）");
        return Ok(());
    }
    let log_path = std::path::PathBuf::from(&log);
    let mut off: u64 = 0;
    loop {
        drain_log(&log_path, &mut off).await;
        let status = match try_call("job.status", serde_json::json!({"job_id": job_id})).await {
            Ok(r) if r.ok => r.data.unwrap_or_default(),
            Ok(r) => {
                drain_log(&log_path, &mut off).await;
                return Err(anyhow!(r.error.unwrap_or_else(|| "job 查询失败".into())));
            }
            Err(e) => {
                return Err(anyhow!(
                    "server 失联，job 可能仍在运行（日志 {log}）: {e}"
                ));
            }
        };
        let running = status
            .get("running")
            .and_then(Value::as_bool)
            .unwrap_or(true);
        if !running {
            drain_log(&log_path, &mut off).await;
            let ok = status
                .get("ok")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let err = status
                .get("error")
                .and_then(|v| v.as_str())
                .map(String::from);
            return match (ok, err) {
                (true, _) => Ok(()),
                (false, Some(e)) => Err(anyhow!(e)),
                (false, None) => Err(anyhow!("job 失败")),
            };
        }
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    }
}
