use std::sync::Arc;
use tokio::task_local;

task_local! {
    static JOB_LOG: Option<Arc<std::sync::Mutex<std::fs::File>>>;
}

/// 在指定日志文件内执行 future：期间 `say!` 输出写入文件而非 stdout
/// 失败时先把错误写进日志（task_local 只在 scope 内生效，写在外面会打到 stdout）
pub async fn scope<F>(path: std::path::PathBuf, fut: F) -> anyhow::Result<()>
where
    F: std::future::Future<Output = anyhow::Result<()>>,
{
    JOB_LOG
        .scope(open_log_file(path), async {
            match fut.await {
                Ok(()) => Ok(()),
                Err(e) => {
                    say(format_args!("[失败] {e:#}"));
                    Err(e)
                }
            }
        })
        .await
}

fn open_log_file(path: std::path::PathBuf) -> Option<Arc<std::sync::Mutex<std::fs::File>>> {
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .ok()
        .map(|f| Arc::new(std::sync::Mutex::new(f)))
}

/// 统一输出入口：job 内写 job 日志，否则写 stdout
pub fn say(args: std::fmt::Arguments<'_>) {
    let written = JOB_LOG
        .try_with(|slot| {
            if let Some(f) = slot {
                use std::io::Write;
                let mut g = f.lock().unwrap_or_else(|p| p.into_inner());
                let _ = writeln!(g, "{args}");
                true
            } else {
                false
            }
        })
        .unwrap_or(false);
    if !written {
        println!("{args}");
    }
}

/// 领域流程统一输出宏（job 内落日志文件，CLI 内落 stdout）
#[macro_export]
macro_rules! say {
    ($($arg:tt)*) => { $crate::joblog::say(format_args!($($arg)*)) };
}
