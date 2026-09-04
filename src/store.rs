use anyhow::Result;
use rusqlite::{Connection, params, params_from_iter};
use std::path::{Path, PathBuf};

use crate::model::{AppState, SubConfig, resolve_sub_names};

const SCHEMA_VERSION: i32 = 2;

/// 当前目录名；LEGACY_DIR_NAME 为更名前残留，一次性迁移用
const DIR_NAME: &str = "proxytool";
const LEGACY_DIR_NAME: &str = "v2ray-cli";

/// 一次性迁移旧目录（v2ray-cli -> proxytool）：搬目录 + 重写 running 表里的绝对路径
/// 新目录已存在则跳过（幂等，可重复跑）
pub fn migrate_legacy_dirs() {
    let old_data = dirs::data_local_dir().map(|d| d.join(LEGACY_DIR_NAME));
    let new_data = dirs::data_local_dir().map(|d| d.join(DIR_NAME));
    let old_cfg = dirs::config_dir().map(|d| d.join(LEGACY_DIR_NAME));
    let new_cfg = dirs::config_dir().map(|d| d.join(DIR_NAME));
    let mut moved_data = false;
    if let (Some(o), Some(n)) = (&old_data, &new_data)
        && o.exists()
        && !n.exists()
    {
        match std::fs::rename(o, n) {
            Ok(()) => {
                println!("已迁移数据目录 {} -> {}", o.display(), n.display());
                moved_data = true;
            }
            Err(e) => eprintln!("迁移数据目录失败 {} -> {}: {e}", o.display(), n.display()),
        }
    }
    if let (Some(o), Some(n)) = (&old_cfg, &new_cfg)
        && o.exists()
        && !n.exists()
    {
        match std::fs::rename(o, n) {
            Ok(()) => println!("已迁移配置目录 {} -> {}", o.display(), n.display()),
            Err(e) => eprintln!("迁移配置目录失败 {} -> {}: {e}", o.display(), n.display()),
        }
    }
    if moved_data && let (Some(o), Some(n)) = (old_data, new_data) {
        // running 表存的是绝对路径，前缀指向旧目录的改写掉（进程本身不受影响）
        let old_p = o.to_string_lossy().to_string();
        let new_p = n.to_string_lossy().to_string();
        let patched = open_db_at(&db_path())
            .and_then(|conn| load_state_from_conn(&conn).map(|st| (conn, st)))
            .map(|(conn, mut st)| {
                let mut changed = false;
                for r in st.running.iter_mut() {
                    if let Some(rest) = r.config_path.strip_prefix(&old_p) {
                        r.config_path = format!("{new_p}{rest}");
                        changed = true;
                    }
                    if let Some(rest) = r.log_path.strip_prefix(&old_p) {
                        r.log_path = format!("{new_p}{rest}");
                        changed = true;
                    }
                }
                if changed {
                    let _ = save_state_to_conn(&conn, &st);
                }
                changed
            });
        match patched {
            Ok(true) => println!("已更新运行态中的旧路径前缀"),
            Ok(false) => {}
            Err(e) => eprintln!("更新运行态路径失败: {e:#}"),
        }
    }
}

pub fn data_dir() -> PathBuf {
    if let Some(d) = dirs::data_local_dir() {
        d.join(DIR_NAME)
    } else {
        PathBuf::from(".")
    }
}

pub fn config_dir() -> PathBuf {
    if let Some(d) = dirs::config_dir() {
        d.join(DIR_NAME)
    } else {
        PathBuf::from(".")
    }
}

pub fn default_subs_path() -> PathBuf {
    config_dir().join("subs.json")
}

/// SQLite 主库路径
pub fn db_path() -> PathBuf {
    data_dir().join("state.db")
}

/// server 的 Unix socket 路径（RPC 通道）
pub fn socket_path() -> PathBuf {
    data_dir().join("server.sock")
}

pub fn open_db_at(path: &Path) -> Result<Connection> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let conn = Connection::open(path)?;
    // 写冲突/初始化竞争等待，避免偶发 database is locked
    conn.busy_timeout(std::time::Duration::from_millis(3000))?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    init_schema(&conn)?;
    Ok(conn)
}

pub fn init_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(&format!(
        "
        PRAGMA user_version = {SCHEMA_VERSION};
        CREATE TABLE IF NOT EXISTS nodes (
            id TEXT PRIMARY KEY,
            sub TEXT NOT NULL DEFAULT '',
            proto TEXT NOT NULL,
            addr TEXT NOT NULL DEFAULT '',
            port INTEGER NOT NULL DEFAULT 0,
            cred TEXT NOT NULL DEFAULT '',
            delay_ms INTEGER NOT NULL DEFAULT -1,
            alive INTEGER NOT NULL DEFAULT 0,
            exit_ip TEXT,
            cc TEXT,
            speed_kbps REAL,
            last_test_at TEXT,
            probed INTEGER NOT NULL DEFAULT 0
        );
        -- 端点唯一（空 addr/0 端口不参与，沿用内存去重兜底语义）
        CREATE UNIQUE INDEX IF NOT EXISTS idx_nodes_endpoint
            ON nodes(lower(addr), port) WHERE addr != '' AND port != 0;
        CREATE INDEX IF NOT EXISTS idx_nodes_alive_delay ON nodes(alive, delay_ms);
        CREATE TABLE IF NOT EXISTS subs (
            name TEXT PRIMARY KEY,
            updated_at TEXT
        );
        CREATE TABLE IF NOT EXISTS running (
            port INTEGER PRIMARY KEY,
            node_id TEXT NOT NULL,
            pid INTEGER NOT NULL,
            config_path TEXT NOT NULL,
            log_path TEXT NOT NULL,
            started_at TEXT
        );
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        "
    ))?;
    // v1 -> v2：存量库补 probed 列
    let has_probed: bool = conn
        .prepare("SELECT 1 FROM pragma_table_info('nodes') WHERE name='probed'")
        .map(|mut q| q.exists([]).unwrap_or(false))
        .unwrap_or(false);
    if !has_probed {
        conn.execute_batch("ALTER TABLE nodes ADD COLUMN probed INTEGER NOT NULL DEFAULT 0;")?;
    }
    Ok(())
}

fn in_txn(conn: &Connection, f: impl FnOnce() -> Result<()>) -> Result<()> {
    conn.execute_batch("BEGIN IMMEDIATE")?;
    match f() {
        Ok(()) => {
            conn.execute_batch("COMMIT")?;
            Ok(())
        }
        Err(e) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(e)
        }
    }
}

fn dt_to_str(d: &Option<chrono::DateTime<chrono::Utc>>) -> Option<String> {
    d.map(|x| x.to_rfc3339())
}

fn delete_missing(conn: &Connection, table: &str, column: &str, values: &[String]) -> Result<()> {
    if values.is_empty() {
        conn.execute(&format!("DELETE FROM {table}"), [])?;
        return Ok(());
    }
    let placeholders = std::iter::repeat_n("?", values.len())
        .collect::<Vec<_>>()
        .join(",");
    conn.execute(
        &format!("DELETE FROM {table} WHERE {column} NOT IN ({placeholders})"),
        params_from_iter(values),
    )?;
    Ok(())
}

fn str_to_dt(s: Option<String>) -> Option<chrono::DateTime<chrono::Utc>> {
    s.and_then(|x| {
        chrono::DateTime::parse_from_rfc3339(&x)
            .ok()
            .map(|d| d.with_timezone(&chrono::Utc))
    })
}

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
                    if n.alive { 1 } else { 0 },
                    n.exit_ip,
                    n.cc,
                    n.speed_kbps,
                    dt_to_str(&n.last_test_at),
                    if n.probed { 1 } else { 0 },
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
                    r.pid as i64,
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

/// 直读（CLI 读命令 / server 启动）；WAL 下与单写者并发安全
pub fn load_state() -> Result<AppState> {
    let conn = open_db_at(&db_path())?;
    let mut st = load_state_from_conn(&conn)?;
    st.settings = crate::config::load_or_create()?;
    Ok(st)
}

// ---- 单写者线程用的 targeted SQL（B 架构下只有 db 线程执行写）----

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

/// 读取 subs.json 清单 `[{name?,url}]`，空名按序号补齐；文件不存在则报错提示
pub fn load_subs_config(path: &std::path::Path) -> Result<Vec<(String, String)>> {
    let s = std::fs::read_to_string(path).map_err(|_| {
        anyhow::anyhow!(
            "订阅配置不存在: {}，请创建 [{{\"name\",\"url\"}}] 格式的 JSON",
            path.display()
        )
    })?;
    let cfgs: Vec<SubConfig> = serde_json::from_str(&s)
        .map_err(|e| anyhow::anyhow!("订阅配置解析失败 {}: {e}", path.display()))?;
    let resolved = resolve_sub_names(&cfgs);
    Ok(resolved
        .into_iter()
        .filter(|(_, u)| !u.trim().is_empty())
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_resolve_via_loader() {
        let dir = std::env::temp_dir().join(format!("proxytool-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("subs.json");
        std::fs::write(
            &p,
            r#"[{"url":"http://a"},{"name":"","url":"http://b"},{"name":"my","url":"http://c"}]"#,
        )
        .unwrap();
        let v = load_subs_config(&p).unwrap();
        assert_eq!(v[0].0, "1");
        assert_eq!(v[1].0, "2");
        assert_eq!(v[2].0, "my");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_missing_config_errors() {
        let p = std::env::temp_dir().join(format!(
            "proxytool-missing-{}-{}.json",
            std::process::id(),
            rand_suffix()
        ));
        assert!(load_subs_config(&p).is_err());
    }

    #[test]
    fn test_load_filters_dead_running_entries() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();
        let mut st = AppState::default();
        // pid=0（历史遗留）与不存在的 pid 都应被过滤
        for (port, pid) in [(10808u16, 0u32), (10809, u32::MAX - 1)] {
            st.running.push(crate::model::RunningProxy {
                port,
                node_id: format!("n{port}"),
                pid,
                config_path: String::new(),
                log_path: String::new(),
                started_at: None,
            });
        }
        save_state_to_conn(&conn, &st).unwrap();
        let back = load_state_from_conn(&conn).unwrap();
        assert!(back.running.is_empty(), "死条目应被过滤");
    }

    #[test]
    fn test_load_keeps_alive_running_entries() {
        // 用名字含 sing-box 的假进程模拟存活（is_pid_alive 校验 cmdline 含 sing-box）
                let dir = std::env::temp_dir().join(format!("proxytool-fake-sb-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let fake = dir.join("sing-box");
        // argv[0] 含 sing-box 且进程活 30s（is_pid_alive 读 /proc/<pid>/cmdline）
        std::fs::write(&fake, "#!/bin/sh\nsleep 30\n").unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
        let mut child = std::process::Command::new(&fake).spawn().unwrap();
        // 等 exec 完成，确保 cmdline 稳定
        std::thread::sleep(std::time::Duration::from_millis(200));

        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();
        let mut st = AppState::default();
        st.running.push(crate::model::RunningProxy {
            port: 10808,
            node_id: "n1".into(),
            pid: child.id(),
            config_path: String::new(),
            log_path: String::new(),
            started_at: None,
        });
        save_state_to_conn(&conn, &st).unwrap();
        let back = load_state_from_conn(&conn).unwrap();
        assert_eq!(back.running.len(), 1, "存活条目应保留");
        assert_eq!(back.running[0].node_id, "n1");

        let _ = child.kill();
        let _ = child.wait();
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn rand_suffix() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        format!(
            "{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )
    }
}

#[cfg(test)]
mod sqlite_tests {
    use super::*;
    use crate::model::{Node, NodeType};
    use rusqlite::Connection;

    fn test_node(id: &str, sub: &str, addr: &str, port: u16) -> Node {
        let mut n = Node::new(
            sub,
            NodeType::Vless,
            addr,
            port,
            &format!("vless://u@{addr}:{port}#{id}"),
        );
        n.id = id.to_string();
        n.delay_ms = 100;
        n.alive = true;
        n.exit_ip = Some("9.9.9.9".into());
        n.speed_kbps = Some(12.5);
        n.probed = true;
        n
    }

    #[test]
    fn test_delete_nodes_removes_only_listed() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();
        upsert_nodes_conn(
            &conn,
            &[
                test_node("keep", "s", "1.1.1.1", 443),
                test_node("drop1", "s", "2.2.2.2", 443),
                test_node("drop2", "s", "3.3.3.3", 443),
            ],
        )
        .unwrap();
        delete_nodes_conn(&conn, &["drop1".to_string(), "drop2".to_string()]).unwrap();
        let st = load_state_from_conn(&conn).unwrap();
        assert_eq!(st.nodes.len(), 1);
        assert_eq!(st.nodes[0].id, "keep");
        // 删不存在的 id 不报错
        delete_nodes_conn(&conn, &["ghost".to_string()]).unwrap();
    }

    #[test]
    fn test_node_roundtrip() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();
        let n = test_node("id1", "barry", "1.1.1.1", 443);
        upsert_nodes_conn(&conn, &[n]).unwrap();
        let st = load_state_from_conn(&conn).unwrap();
        assert_eq!(st.nodes.len(), 1);
        let back = &st.nodes[0];
        assert_eq!(back.id, "id1");
        assert_eq!(back.sub, "barry");
        assert_eq!(back.addr, "1.1.1.1");
        assert_eq!(back.exit_ip.as_deref(), Some("9.9.9.9"));
        assert_eq!(back.speed_kbps, Some(12.5));
        assert!(back.probed);
    }

    #[test]
    fn test_endpoint_unique_partial() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();
        // 同端点保留首次
        upsert_nodes_conn(&conn, &[test_node("a", "s", "2.2.2.2", 443)]).unwrap();
        upsert_nodes_conn(&conn, &[test_node("b", "s", "2.2.2.2", 443)]).unwrap();
        let st = load_state_from_conn(&conn).unwrap();
        assert_eq!(st.nodes.len(), 1);
        assert_eq!(st.nodes[0].id, "a");
        // 空 addr 互不合并
        upsert_nodes_conn(
            &conn,
            &[test_node("c", "s", "", 443), test_node("d", "s", "", 443)],
        )
        .unwrap();
        let st = load_state_from_conn(&conn).unwrap();
        assert_eq!(st.nodes.len(), 3);
    }

    #[test]
    fn test_targeted_running_and_meta_ops() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();
        // running 小函数（批量版）
        let r = crate::model::RunningProxy {
            port: 18282,
            node_id: "id1".into(),
            pid: 123,
            config_path: "/tmp/a.json".into(),
            log_path: "/tmp/a.log".into(),
            started_at: None,
        };
        set_runnings_conn(&conn, &[r]).unwrap();
        set_running_node_conn(&conn, 18282, "id2").unwrap();
        // load_state_from_conn 会过滤死 pid，这里直接查行验证小函数语义
        let node_id: String = conn
            .query_row(
                "SELECT node_id FROM running WHERE port=18282",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(node_id, "id2");
        remove_runnings_conn(&conn, &[18282]).unwrap();
        let cnt: i64 = conn
            .query_row("SELECT COUNT(*) FROM running", [], |row| row.get(0))
            .unwrap();
        assert_eq!(cnt, 0);
        // meta 小函数
        set_meta_conn(&conn, "server.pid", Some("123")).unwrap();
        set_meta_conn(&conn, "watch:19999", Some(r#"{"verify_url":"x"}"#)).unwrap();
        set_meta_conn(&conn, "gone", None).unwrap();
        let st = load_state_from_conn(&conn).unwrap();
        assert_eq!(st.meta.get("server.pid").map(String::as_str), Some("123"));
        assert!(st.meta.contains_key("watch:19999"));
        assert!(!st.meta.contains_key("gone"));
    }

    #[test]
    fn test_batch_running_ops() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();
        let mk = |port: u16, node: &str| crate::model::RunningProxy {
            port,
            node_id: node.into(),
            pid: 123,
            config_path: format!("/tmp/{port}.json"),
            log_path: format!("/tmp/{port}.log"),
            started_at: None,
        };
        // 批量插入多行
        set_runnings_conn(&conn, &[mk(10808, "a"), mk(10809, "b")]).unwrap();
        // 同端口批量 upsert 覆盖
        set_runnings_conn(&conn, &[mk(10808, "c")]).unwrap();
        let node_id: String = conn
            .query_row("SELECT node_id FROM running WHERE port=10808", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(node_id, "c");
        // 批量删除只删指定端口
        remove_runnings_conn(&conn, &[10808]).unwrap();
        let ports: Vec<i64> = conn
            .prepare("SELECT port FROM running")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(ports, vec![10809]);
        // 空批量不报错
        set_runnings_conn(&conn, &[]).unwrap();
        remove_runnings_conn(&conn, &[]).unwrap();
    }

    #[test]
    fn test_mark_dead_clears_observations() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();
        let mut n = test_node("id1", "s", "1.1.1.1", 443);
        n.delay_ms = 50;
        upsert_nodes_conn(&conn, &[n]).unwrap();
        mark_dead_conn(&conn, "id1").unwrap();
        let back = &load_state_from_conn(&conn).unwrap().nodes[0];
        assert!(!back.alive && back.delay_ms == -1 && back.speed_kbps.is_none());
        assert!(back.exit_ip.is_none() && back.cc.is_none() && !back.probed);
    }

    #[test]
    fn test_save_state_reconciles_existing_rows_in_place() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();
        let first = test_node("id1", "barry", "1.1.1.1", 443);
        let retained = test_node("id2", "barry", "2.2.2.2", 443);
        upsert_nodes_conn(&conn, &[first, retained.clone()]).unwrap();
        let rowid_before: i64 = conn
            .query_row("SELECT rowid FROM nodes WHERE id='id2'", [], |row| {
                row.get(0)
            })
            .unwrap();
        let mut updated = retained;
        updated.delay_ms = 200;
        let mut state = AppState::default();
        state.nodes.push(updated);
        save_state_to_conn(&conn, &state).unwrap();
        let rowid_after: i64 = conn
            .query_row("SELECT rowid FROM nodes WHERE id='id2'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(rowid_after, rowid_before);
        assert_eq!(load_state_from_conn(&conn).unwrap().nodes[0].delay_ms, 200);
    }
}
