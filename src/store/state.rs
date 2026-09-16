use anyhow::Result;
use rusqlite::{Connection, params};

use crate::model::AppState;
use super::{delete_missing, dt_to_str, in_txn, str_to_dt};

/// 全量同步（sub update/prune 等批量路径，原子事务）
pub fn save_state_to_conn(conn: &Connection, st: &AppState) -> Result<()> {
    in_txn(conn, || {
        delete_missing(
            conn,
            "nodes",
            "id",
            &st.nodes
                .iter()
                .map(|node| node.id.clone())
                .collect::<Vec<_>>(),
        )?;
        {
            let mut upsert = conn.prepare(
                "INSERT INTO nodes (id, sub, proto, addr, port, cred, delay_ms, alive, exit_ip, cc, speed_kbps, last_test_at, probed)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)
                 ON CONFLICT(id) DO UPDATE SET
                   sub=excluded.sub, proto=excluded.proto, addr=excluded.addr, port=excluded.port,
                   cred=excluded.cred, delay_ms=excluded.delay_ms, alive=excluded.alive,
                   exit_ip=excluded.exit_ip, cc=excluded.cc, speed_kbps=excluded.speed_kbps,
                   last_test_at=excluded.last_test_at, probed=excluded.probed",
            )?;
            for n in &st.nodes {
                upsert.execute(params![
                    n.id,
                    n.sub,
                    n.r#type.as_str(),
                    n.addr,
                    n.port,
                    n.cred,
                    n.delay_ms,
                    i32::from(n.alive),
                    n.exit_ip,
                    n.cc,
                    n.speed_kbps,
                    dt_to_str(&n.last_test_at),
                    i32::from(n.probed),
                ])?;
            }
        }
        delete_missing(
            conn,
            "subs",
            "name",
            &st.subs
                .iter()
                .map(|sub| sub.name.clone())
                .collect::<Vec<_>>(),
        )?;
        {
            let mut upsert = conn.prepare(
                "INSERT INTO subs (name, updated_at) VALUES (?1,?2)
                 ON CONFLICT(name) DO UPDATE SET updated_at=excluded.updated_at",
            )?;
            for s in &st.subs {
                upsert.execute(params![s.name, dt_to_str(&s.updated_at)])?;
            }
        }
        delete_missing(
            conn,
            "running",
            "port",
            &st.running
                .iter()
                .map(|running| running.port.to_string())
                .collect::<Vec<_>>(),
        )?;
        {
            let mut upsert = conn.prepare(
                "INSERT INTO running (port, node_id, pid, config_path, log_path, started_at)
                 VALUES (?1,?2,?3,?4,?5,?6)
                 ON CONFLICT(port) DO UPDATE SET
                   node_id=excluded.node_id, pid=excluded.pid, config_path=excluded.config_path,
                   log_path=excluded.log_path, started_at=excluded.started_at",
            )?;
            for r in &st.running {
                upsert.execute(params![
                    r.port,
                    r.node_id,
                    i64::from(r.pid),
                    r.config_path,
                    r.log_path,
                    dt_to_str(&r.started_at),
                ])?;
            }
        }
        delete_missing(conn, "meta", "key", &st.meta.keys().cloned().collect::<Vec<_>>())?;
        {
            let mut upsert = conn.prepare(
                "INSERT INTO meta (key, value) VALUES (?1, ?2)
                 ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            )?;
            for (k, v) in &st.meta {
                upsert.execute(params![k, v])?;
            }
        }
        Ok(())
    })
}

pub fn load_state_from_conn(conn: &Connection) -> Result<AppState> {
    let mut st = AppState::default();
    {
        let mut q = conn.prepare(
            "SELECT id, sub, proto, addr, port, cred, delay_ms, alive, exit_ip, cc, speed_kbps, last_test_at, probed FROM nodes",
        )?;
        let rows = q.query_map([], |row| {
            Ok(crate::model::Node {
                id: row.get(0)?,
                sub: row.get(1)?,
                r#type: crate::model::NodeType::from_scheme(&row.get::<_, String>(2)?),
                addr: row.get(3)?,
                port: row.get::<_, i64>(4)? as u16,
                cred: row.get(5)?,
                delay_ms: row.get(6)?,
                alive: row.get::<_, i64>(7)? != 0,
                exit_ip: row.get(8)?,
                cc: row.get(9)?,
                speed_kbps: row.get(10)?,
                last_test_at: str_to_dt(row.get(11)?),
                probed: row.get::<_, i64>(12)? != 0,
            })
        })?;
        for n in rows {
            st.nodes.push(n?);
        }
    }
    {
        let mut q = conn.prepare("SELECT name, updated_at FROM subs")?;
        let rows = q.query_map([], |row| {
            Ok(crate::model::SubMeta {
                name: row.get(0)?,
                updated_at: str_to_dt(row.get::<_, Option<String>>(1)?),
            })
        })?;
        for s in rows {
            st.subs.push(s?);
        }
    }
    {
        let mut q = conn
            .prepare("SELECT port, node_id, pid, config_path, log_path, started_at FROM running")?;
        let rows = q.query_map([], |row| {
            Ok(crate::model::RunningProxy {
                port: row.get::<_, i64>(0)? as u16,
                node_id: row.get(1)?,
                pid: row.get::<_, i64>(2)? as u32,
                config_path: row.get(3)?,
                log_path: row.get(4)?,
                started_at: str_to_dt(row.get::<_, Option<String>>(5)?),
            })
        })?;
        for r in rows {
            st.running.push(r?);
        }
    }
    {
        let mut q = conn.prepare("SELECT key, value FROM meta")?;
        let rows = q.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        for row in rows {
            let (k, v) = row?;
            st.meta.insert(k, v);
        }
    }
    // 运行态清理：pid=0（历史遗留）或进程已死的条目直接丢弃，避免残留行误导 status/switch
    st.running.retain(|r| crate::run::is_pid_alive(r.pid));
    Ok(st)
}
