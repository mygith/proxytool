use anyhow::{Result, anyhow};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::{
    net::TcpStream,
    time::{Duration, sleep, timeout},
};

use crate::store;

static CONFIG_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// 单次启动的产物（pid=0 表示无实际进程：未装 sing-box，仅生成配置）
pub struct Launched {
    pub pid: u32,
    pub config_path: PathBuf,
}

pub fn generate_config_path() -> PathBuf {
    let dir = store::data_dir();
    std::fs::create_dir_all(&dir).ok();
    let seq = CONFIG_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    dir.join(format!(
        "singbox-{}-{}-{seq}.json",
        chrono::Utc::now().timestamp_millis(),
        std::process::id()
    ))
}

/// 端口当前能否连上（单次探测 100ms）
async fn port_open(port: u16) -> bool {
    timeout(
        Duration::from_millis(100),
        TcpStream::connect(format!("127.0.0.1:{port}")),
    )
    .await
    .is_ok_and(|result| result.is_ok())
}

pub async fn wait_for_ports(ports: &[u16], timeout_ms: u64) -> bool {
    let start = std::time::Instant::now();
    loop {
        let mut all_ok = true;
        for &port in ports {
            if !port_open(port).await {
                all_ok = false;
                break;
            }
        }
        if all_ok {
            return true;
        }
        if start.elapsed().as_millis() > timeout_ms as u128 {
            return false;
        }
        sleep(Duration::from_millis(50)).await;
    }
}

/// 端口当前是否全部空闲（单次探测，不等待）
pub async fn are_ports_free(ports: &[u16]) -> bool {
    for &port in ports {
        if port_open(port).await {
            return false;
        }
    }
    true
}

pub async fn wait_for_ports_free(ports: &[u16], timeout_ms: u64) -> bool {
    let start = std::time::Instant::now();
    loop {
        if are_ports_free(ports).await {
            return true;
        }
        if start.elapsed().as_millis() > timeout_ms as u128 {
            return false;
        }
        sleep(Duration::from_millis(100)).await;
    }
}

pub async fn wait_for_pid_gone(pid: u32, timeout_ms: u64) -> bool {
    let start = std::time::Instant::now();
    loop {
        if !is_pid_alive(pid) {
            return true;
        }
        if start.elapsed().as_millis() > timeout_ms as u128 {
            return !is_pid_alive(pid);
        }
        sleep(Duration::from_millis(100)).await;
    }
}

/// pid 是否为存活的 sing-box 进程（校验 cmdline 防 pid 复用误判）
pub fn is_pid_alive(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    let base = format!("/proc/{pid}");
    if !std::path::Path::new(&base).exists() {
        return false;
    }
    match std::fs::read(format!("{base}/cmdline")) {
        Ok(b) => {
            let s = String::from_utf8_lossy(&b).to_ascii_lowercase();
            s.contains("sing-box")
        }
        Err(_) => false,
    }
}

/// pid 是否为存活的 proxytool 进程（校验 cmdline 防 pid 复用误判）
pub fn is_tool_alive(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    let base = format!("/proc/{pid}");
    if !std::path::Path::new(&base).exists() {
        return false;
    }
    match std::fs::read(format!("{base}/cmdline")) {
        Ok(b) => {
            let s = String::from_utf8_lossy(&b).to_ascii_lowercase();
            s.contains("proxytool")
        }
        Err(_) => false,
    }
}

pub fn kill_pid(pid: u32, force: bool) -> Result<()> {
    let sig = if force { "-KILL" } else { "-TERM" };
    let st = std::process::Command::new("kill")
        .arg(sig)
        .arg(pid.to_string())
        .status()
        .map_err(|e| anyhow!("执行 kill 失败: {e}"))?;
    if st.success() {
        Ok(())
    } else {
        Err(anyhow!("kill {pid} 退出码非零"))
    }
}

/// 取文件末尾 n 行（日志排障用）
pub fn tail_file(path: &std::path::Path, n: usize) -> String {
    let Ok(s) = std::fs::read_to_string(path) else {
        return String::new();
    };
    let lines: Vec<&str> = s.lines().collect();
    let start = lines.len().saturating_sub(n.max(1));
    lines[start..].join("\n")
}

/// 清理陈旧的 <prefix>* 历史文件，仅保留最新的 keep 个（当前使用的除外）
pub fn prune_old_files(dir: &std::path::Path, keep: usize, prefix: &str, exclude: &[PathBuf]) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return;
    };
    let mut groups: std::collections::HashMap<String, (std::time::SystemTime, Vec<PathBuf>)> =
        std::collections::HashMap::new();
    for e in rd.flatten() {
        let p = e.path();
        let name = p.file_name().and_then(|x| x.to_str()).unwrap_or("");
        if !name.starts_with(prefix) {
            continue;
        }
        if exclude.iter().any(|x| x == &p) {
            continue;
        }
        let mtime = e
            .metadata()
            .and_then(|m| m.modified())
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        let group = p
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or(name)
            .to_string();
        let entry = groups
            .entry(group)
            .or_insert_with(|| (std::time::SystemTime::UNIX_EPOCH, Vec::new()));
        entry.0 = entry.0.max(mtime);
        entry.1.push(p);
    }
    let mut groups: Vec<_> = groups.into_values().collect();
    groups.sort_by_key(|x| std::cmp::Reverse(x.0)); // 新在前
    for (_, paths) in groups.into_iter().skip(keep.max(1)) {
        for p in paths {
            let _ = std::fs::remove_file(&p);
        }
    }
}

/// stop 目标选择（纯函数）：--all 全停；--port 停指定；都不给且仅 1 个在运行则停它
pub fn select_stop_indices(ports: &[u16], port: Option<u16>, all: bool) -> Result<Vec<usize>> {
    if all {
        if ports.is_empty() {
            return Err(anyhow!("无运行中代理"));
        }
        return Ok((0..ports.len()).collect());
    }
    if let Some(p) = port {
        let idx: Vec<usize> = ports
            .iter()
            .enumerate()
            .filter(|(_, x)| **x == p)
            .map(|(i, _)| i)
            .collect();
        if idx.is_empty() {
            return Err(anyhow!("端口 {p} 不在运行中"));
        }
        return Ok(idx);
    }
    if ports.len() == 1 {
        return Ok(vec![0]);
    }
    Err(anyhow!("请指定 --port <port> 或 --all"))
}

#[cfg(test)]
mod run_new_tests {
    use super::*;
    #[test]
    fn test_pid_alive_self_and_bogus() {
        // 自身不是 sing-box 进程，应判死；极大 pid 不存在
        assert!(!is_pid_alive(std::process::id()));
        assert!(!is_pid_alive(0));
        assert!(!is_pid_alive(u32::MAX));
    }

    #[test]
    fn test_tail_file_last_n() {
        let dir = std::env::temp_dir().join(format!("proxytool-tail-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("a.log");
        std::fs::write(&p, "l1\nl2\nl3\nl4\n").unwrap();
        let t = tail_file(&p, 2);
        assert!(t.contains("l3") && t.contains("l4") && !t.contains("l1"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_are_ports_free_detects_occupied() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        assert!(!are_ports_free(&[port]).await);
        drop(listener);
        assert!(are_ports_free(&[port]).await);
    }

    #[test]
    fn test_generated_config_paths_are_unique() {
        let a = generate_config_path();
        let b = generate_config_path();
        assert_ne!(a, b);
    }

    #[test]
    fn test_prune_old_files_keeps_complete_groups() {
        let dir = std::env::temp_dir().join(format!("proxytool-prune-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        for id in 1..=3 {
            std::fs::write(dir.join(format!("singbox-{id}.json")), "{}").unwrap();
            std::fs::write(dir.join(format!("singbox-{id}.log")), "log").unwrap();
        }
        prune_old_files(&dir, 2, "singbox-", &[]);
        let kept = std::fs::read_dir(&dir).unwrap().count();
        assert_eq!(kept, 4);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_select_stop_targets() {
        let ports = vec![10808u16, 10809];
        assert_eq!(select_stop_indices(&ports, None, true).unwrap(), vec![0, 1]);
        assert_eq!(
            select_stop_indices(&ports, Some(10809), false).unwrap(),
            vec![1]
        );
        assert!(select_stop_indices(&ports, Some(9999), false).is_err());
        assert!(select_stop_indices(&ports, None, false).is_err());
        assert_eq!(select_stop_indices(&[10808], None, false).unwrap(), vec![0]);
    }
}
