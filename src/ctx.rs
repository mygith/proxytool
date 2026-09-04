use anyhow::{anyhow, Result};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

use crate::db::DbOp;
use crate::model::{AppState, Node, RunningProxy};
use crate::{joblog, run, store};

/// 端口互斥：同端口同时只能被一个任务操作（auto/run/stop/switch/看护切换），防并发互相拆台
/// std Mutex 足够（只做 HashSet 插删，不跨 await 持有）
#[derive(Debug, Default)]
pub struct PortLocks {
    set: std::sync::Mutex<HashSet<u16>>,
}

/// 持有即加锁，Drop 即释放（同步 Drop，不碰 async）
pub struct PortGuard<'a> {
    ports: Vec<u16>,
    set: &'a std::sync::Mutex<HashSet<u16>>,
}

impl PortLocks {
    pub fn acquire(&self, ports: &[u16]) -> Result<PortGuard<'_>> {
        let mut s = self.set.lock().map_err(|_| anyhow!("端口锁中毒"))?;
        if let Some(p) = ports.iter().find(|p| s.contains(p)) {
            return Err(anyhow!("端口 {p} 正被其他任务操作，稍后重试"));
        }
        s.extend(ports.iter().copied());
        Ok(PortGuard {
            ports: ports.to_vec(),
            set: &self.set,
        })
    }
}

impl Drop for PortGuard<'_> {
    fn drop(&mut self) {
        if let Ok(mut s) = self.set.lock() {
            for p in &self.ports {
                s.remove(p);
            }
        }
    }
}

/// server 运行时上下文：内存态为真源，持久化经单写者线程串行落盘
pub struct Ctx {
    pub state: Arc<RwLock<AppState>>,
    pub db: crate::db::DbHandle,
    pub jobs: Arc<Mutex<HashMap<u64, JobInfo>>>,
    pub job_seq: AtomicU64,
    pub watches: Arc<Mutex<HashMap<u16, tokio::task::JoinHandle<()>>>>,
    pub port_locks: PortLocks,
    /// 写串行化：内存换入与 DB 落盘同序，读快照走 RwLock 读锁全程不阻塞
    pub(crate) write_mu: Mutex<()>,
}

pub struct JobInfo {
    pub log: PathBuf,
    pub running: bool,
    pub ok: Option<bool>,
    pub error: Option<String>,
    pub handle: Option<tokio::task::JoinHandle<()>>,
}

impl Ctx {
    pub async fn snapshot(&self) -> AppState {
        self.state.read().await.clone()
    }

    /// 占住端口（ guard 存活期间有效；同一任务内不可重入，auto 调 stop 走 stop_inner 直调）
    pub fn acquire_ports(&self, ports: &[u16]) -> Result<PortGuard<'_>> {
        self.port_locks.acquire(ports)
    }

    /// 全量替换式变更：内存算好新状态 -> DB 落盘 -> 短暂写锁换入
    /// 状态读锁只在首尾瞬间持有，DB 等待期间读快照不阻塞；write_mu 保证内存与 DB 同序
    pub async fn replace_all(&self, f: impl FnOnce(&mut AppState)) -> Result<()> {
        let _w = self.write_mu.lock().await;
        let mut next = self.state.read().await.clone();
        f(&mut next);
        self.db.exec(DbOp::ReplaceAll(Box::new(next.clone()))).await?;
        *self.state.write().await = next;
        Ok(())
    }

    pub async fn upsert_nodes(&self, nodes: &[Node]) -> Result<()> {
        let _w = self.write_mu.lock().await;
        let mut next = self.state.read().await.clone();
        for n in nodes {
            match next.nodes.iter_mut().find(|x| x.id == n.id) {
                Some(slot) => *slot = n.clone(),
                None => next.nodes.push(n.clone()),
            }
        }
        self.db.exec(DbOp::UpsertNodes(nodes.to_vec())).await?;
        *self.state.write().await = next;
        Ok(())
    }

    pub async fn delete_nodes(&self, ids: &[String]) -> Result<()> {
        let _w = self.write_mu.lock().await;
        let mut next = self.state.read().await.clone();
        next.nodes.retain(|n| !ids.contains(&n.id));
        self.db.exec(DbOp::DeleteNodes(ids.to_vec())).await?;
        *self.state.write().await = next;
        Ok(())
    }

    pub async fn mark_dead(&self, id: &str) -> Result<()> {
        let _w = self.write_mu.lock().await;
        let mut next = self.state.read().await.clone();
        if let Some(n) = next.nodes.iter_mut().find(|x| x.id == id) {
            n.alive = false;
            n.delay_ms = -1;
            n.speed_kbps = None;
            n.exit_ip = None;
            n.cc = None;
            n.probed = false;
        }
        self.db.exec(DbOp::MarkDead(id.to_string())).await?;
        *self.state.write().await = next;
        Ok(())
    }

    pub async fn put_running(&self, entries: Vec<RunningProxy>) -> Result<()> {
        let _w = self.write_mu.lock().await;
        let mut next = self.state.read().await.clone();
        let ports: HashSet<u16> = entries.iter().map(|e| e.port).collect();
        next.running.retain(|r| !ports.contains(&r.port));
        next.running.extend(entries.iter().cloned());
        // 单条 DbOp 原子落盘：失败则内存整体不换入，不残留半批
        self.db.exec(DbOp::SetRunnings(entries)).await?;
        *self.state.write().await = next;
        Ok(())
    }

    pub async fn remove_running(&self, ports: &[u16]) -> Result<()> {
        let _w = self.write_mu.lock().await;
        let mut next = self.state.read().await.clone();
        next.running.retain(|r| !ports.contains(&r.port));
        self.db.exec(DbOp::RemoveRunnings(ports.to_vec())).await?;
        *self.state.write().await = next;
        Ok(())
    }

    pub async fn set_running_node(&self, port: u16, id: &str) -> Result<()> {
        let _w = self.write_mu.lock().await;
        let mut next = self.state.read().await.clone();
        if let Some(r) = next.running.iter_mut().find(|r| r.port == port) {
            r.node_id = id.to_string();
        }
        self.db.exec(DbOp::SetRunningNode(port, id.to_string())).await?;
        *self.state.write().await = next;
        Ok(())
    }

    pub async fn set_meta(&self, key: &str, value: Option<String>) -> Result<()> {
        let _w = self.write_mu.lock().await;
        let mut next = self.state.read().await.clone();
        match &value {
            Some(v) => {
                next.meta.insert(key.to_string(), v.clone());
            }
            None => {
                next.meta.remove(key);
            }
        }
        self.db.exec(DbOp::SetMeta(key.to_string(), value)).await?;
        *self.state.write().await = next;
        Ok(())
    }
}

/// 提交后台 job：输出重定向到专属日志，完成后回写注册表并清理陈旧日志
pub async fn spawn_job<F>(ctx: std::sync::Arc<Ctx>, kind: &str, fut: F) -> Result<(u64, PathBuf)>
where
    F: std::future::Future<Output = Result<()>> + Send + 'static,
{
    let id = ctx.job_seq.fetch_add(1, Ordering::Relaxed) + 1;
    let dir = store::data_dir();
    // pid 进文件名：server 重启后 job id 重新计数，防 append 到旧 server 的同名日志
    let log = dir.join(format!("job-{kind}-{}-{id}.log", std::process::id()));
    ctx.jobs.lock().await.insert(
        id,
        JobInfo {
            log: log.clone(),
            running: true,
            ok: None,
            error: None,
            handle: None,
        },
    );
    let jctx = ctx.clone();
    let klog = log.clone();
    let handle = tokio::spawn(async move {
        let r = joblog::scope(klog.clone(), fut).await;
        let mut jobs = jctx.jobs.lock().await;
        if let Some(j) = jobs.get_mut(&id) {
            j.running = false;
            match r {
                Ok(()) => j.ok = Some(true),
                Err(e) => {
                    j.ok = Some(false);
                    j.error = Some(format!("{e:#}"));
                }
            }
        }
        drop(jobs);
        run::prune_old_files(&store::data_dir(), 20, "job-", &[klog]);
    });
    if let Some(j) = ctx.jobs.lock().await.get_mut(&id) {
        j.handle = Some(handle);
    }
    Ok((id, log))
}

#[cfg(test)]
mod port_lock_tests {
    use super::*;

    #[test]
    fn test_port_lock_exclusive() {
        let l = PortLocks::default();
        let _g = l.acquire(&[10808]).unwrap();
        assert!(l.acquire(&[10808]).is_err());
        assert!(l.acquire(&[10809, 10808]).is_err());
        // 不相交端口不受影响（guard 立即释放）
        assert!(l.acquire(&[10809]).is_ok());
    }

    #[test]
    fn test_port_lock_released_on_drop() {
        let l = PortLocks::default();
        {
            let _g = l.acquire(&[10808]).unwrap();
        }
        assert!(l.acquire(&[10808]).is_ok());
    }
}
