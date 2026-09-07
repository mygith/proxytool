use anyhow::Result;
use rusqlite::Connection;
use tokio::sync::{mpsc, oneshot};

use crate::model::{AppState, Node, RunningProxy};
use crate::store;

/// DB 写操作枚举：单写者线程消费，内存态之外的持久化全部走这里
pub enum DbOp {
    ReplaceAll(Box<AppState>),
    UpsertNodes(Vec<Node>),
    DeleteNodes(Vec<String>),
    MarkDead(String),
    SetRunnings(Vec<RunningProxy>),
    RemoveRunnings(Vec<u16>),
    SetRunningNode(u16, String),
    SetMeta(String, Option<String>),
}

struct DbReq {
    op: DbOp,
    done: oneshot::Sender<Result<()>>,
}

/// DB 写句柄（可 clone），exec 顺序执行保证串行
#[derive(Clone)]
pub struct DbHandle {
    tx: mpsc::Sender<DbReq>,
}

impl DbHandle {
    pub async fn exec(&self, op: DbOp) -> Result<()> {
        let (done_tx, done_rx) = oneshot::channel();
        self.tx
            .send(DbReq { op, done: done_tx })
            .await
            .map_err(|_| anyhow::anyhow!("DB 写线程已退出"))?;
        done_rx
            .await
            .map_err(|_| anyhow::anyhow!("DB 写线程无响应"))?
    }
}

/// 启动单写者线程：唯一拥有 Connection，按序应用 DbOp
pub fn spawn_writer() -> Result<DbHandle> {
    let (tx, mut rx) = mpsc::channel::<DbReq>(256);
    let path = store::db_path();
    std::thread::Builder::new()
        .name("db-writer".into())
        .spawn(move || {
            let conn = match store::open_db_at(&path) {
                Ok(c) => c,
                Err(e) => {
                    while let Some(req) = rx.blocking_recv() {
                        let _ = req.done.send(Err(anyhow::anyhow!("DB 打开失败: {e:#}")));
                    }
                    return;
                }
            };
            let mut ops_since_checkpoint = 0usize;
            while let Some(req) = rx.blocking_recv() {
                let r = apply(&conn, req.op);
                let _ = req.done.send(r);
                // 常驻进程 WAL 只增不减：每 100 次写截断 checkpoint 一次（失败不管，下次再试）
                ops_since_checkpoint += 1;
                if ops_since_checkpoint >= 100 {
                    ops_since_checkpoint = 0;
                    let _ = conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);");
                }
            }
        })
        .map_err(|e| anyhow::anyhow!("启动 DB 写线程失败: {e}"))?;
    Ok(DbHandle { tx })
}

fn apply(conn: &Connection, op: DbOp) -> Result<()> {
    match op {
        DbOp::ReplaceAll(st) => store::save_state_to_conn(conn, &st),
        DbOp::UpsertNodes(nodes) => store::upsert_nodes_conn(conn, &nodes),
        DbOp::DeleteNodes(ids) => store::delete_nodes_conn(conn, &ids),
        DbOp::MarkDead(id) => store::mark_dead_conn(conn, &id),
        DbOp::SetRunnings(entries) => store::set_runnings_conn(conn, &entries),
        DbOp::RemoveRunnings(ports) => store::remove_runnings_conn(conn, &ports),
        DbOp::SetRunningNode(port, id) => store::set_running_node_conn(conn, port, &id),
        DbOp::SetMeta(key, value) => store::set_meta_conn(conn, &key, value.as_deref()),
    }
}
