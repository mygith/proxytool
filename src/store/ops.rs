use anyhow::Result;
use rusqlite::{Connection, params};

use super::{dt_to_str, in_txn};

// ---- 单写者线程用的 targeted SQL（B 架构下只有 db 线程执行写）----

/// 增量 upsert 节点（probe 热路径）：按 id 更新；新 id 若撞端点唯一索引则保留首次（DO NOTHING）
pub fn upsert_nodes_conn(conn: &Connection, nodes: &[crate::model::Node]) -> Result<()> {
    in_txn(conn, || {
        let mut upd = conn.prepare(
            "UPDATE nodes SET sub=?2, proto=?3, addr=?4, port=?5, cred=?6, delay_ms=?7, alive=?8,
             exit_ip=?9, cc=?10, speed_kbps=?11, last_test_at=?12, probed=?13 WHERE id=?1",
        )?;
        let mut ins = conn.prepare(
            "INSERT INTO nodes (id, sub, proto, addr, port, cred, delay_ms, alive, exit_ip, cc, speed_kbps, last_test_at, probed)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13) ON CONFLICT DO NOTHING",
        )?;
        for n in nodes {
            let changed = upd.execute(params![
                n.id,
                n.sub,
                n.r#type.as_str(),
                n.addr,
                n.port,
                n.cred,
                n.delay_ms,
                if n.alive { 1 } else { 0 },
                n.exit_ip,
                n.cc,
                n.speed_kbps,
                dt_to_str(&n.last_test_at),
                if n.probed { 1 } else { 0 },
            ])?;
            if changed == 0 {
                ins.execute(params![
                    n.id,
                    n.sub,
                    n.r#type.as_str(),
                    n.addr,
                    n.port,
                    n.cred,
                    n.delay_ms,
                    if n.alive { 1 } else { 0 },
                    n.exit_ip,
                    n.cc,
                    n.speed_kbps,
                    dt_to_str(&n.last_test_at),
                    if n.probed { 1 } else { 0 },
                ])?;
            }
        }
        Ok(())
    })
}

/// 批量 upsert running 行（单事务；put_running 原子提交用，避免逐条失败分叉）
pub fn set_runnings_conn(conn: &Connection, entries: &[crate::model::RunningProxy]) -> Result<()> {
    in_txn(conn, || {
        let mut up = conn.prepare(
            "INSERT INTO running (port, node_id, pid, config_path, log_path, started_at)
             VALUES (?1,?2,?3,?4,?5,?6)
             ON CONFLICT(port) DO UPDATE SET
               node_id=excluded.node_id, pid=excluded.pid, config_path=excluded.config_path,
               log_path=excluded.log_path, started_at=excluded.started_at",
        )?;
        for r in entries {
            up.execute(params![
                r.port,
                r.node_id,
                r.pid as i64,
                r.config_path,
                r.log_path,
                dt_to_str(&r.started_at),
            ])?;
        }
        Ok(())
    })
}

/// 批量删除 running 行（单事务；remove_running 原子提交用）
pub fn remove_runnings_conn(conn: &Connection, ports: &[u16]) -> Result<()> {
    in_txn(conn, || {
        let mut del = conn.prepare("DELETE FROM running WHERE port=?1")?;
        for p in ports {
            del.execute(params![p])?;
        }
        Ok(())
    })
}

pub fn set_running_node_conn(conn: &Connection, port: u16, id: &str) -> Result<()> {
    conn.execute(
        "UPDATE running SET node_id=?2 WHERE port=?1",
        params![port, id],
    )?;
    Ok(())
}

/// 标死节点（清观测字段，switch/watch/流式替换共用语义）
pub fn mark_dead_conn(conn: &Connection, id: &str) -> Result<()> {
    conn.execute(
        "UPDATE nodes SET alive=0, delay_ms=-1, speed_kbps=NULL, exit_ip=NULL, cc=NULL, probed=0
         WHERE id=?1",
        params![id],
    )?;
    Ok(())
}

pub fn set_meta_conn(conn: &Connection, key: &str, value: Option<&str>) -> Result<()> {
    match value {
        Some(v) => conn.execute(
            "INSERT INTO meta (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            params![key, v],
        )?,
        None => conn.execute("DELETE FROM meta WHERE key=?1", params![key])?,
    };
    Ok(())
}

pub fn delete_nodes_conn(conn: &Connection, ids: &[String]) -> Result<()> {
    in_txn(conn, || {
        let mut del = conn.prepare("DELETE FROM nodes WHERE id=?1")?;
        for id in ids {
            del.execute(params![id])?;
        }
        Ok(())
    })
}
