use anyhow::Result;
use rusqlite::{Connection, params_from_iter};
use std::path::{Path, PathBuf};

use crate::model::{AppState, SubConfig, expand_date_url, resolve_sub_names};

mod schema;
mod state;
mod ops;

pub use schema::init_schema;
pub use state::{load_state_from_conn, save_state_to_conn};
pub use ops::{
    mark_dead_conn, remove_runnings_conn, set_meta_conn, set_running_node_conn, set_runnings_conn,
    upsert_nodes_conn,
};

const DIR_NAME: &str = "proxytool";

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

/// server 日志路径（固定名，启动时清空）
pub fn serve_log_path() -> PathBuf {
    logs_dir().join("serve.log")
}

/// job 日志路径（固定名；同一时刻只允许一个 job，故可共用）
pub fn job_log_path() -> PathBuf {
    logs_dir().join("job.log")
}

/// 日志目录（data_dir/logs），首次调用时创建
pub fn logs_dir() -> PathBuf {
    let d = data_dir().join("logs");
    std::fs::create_dir_all(&d).ok();
    d
}

/// sing-box 配置与日志目录（logs/singbox）
/// 配置与日志同目录：prune_old_files 按文件名主干分组，二者才能同生共死
pub fn singbox_dir() -> PathBuf {
    let d = logs_dir().join("singbox");
    std::fs::create_dir_all(&d).ok();
    d
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

pub fn in_txn(conn: &Connection, f: impl FnOnce() -> Result<()>) -> Result<()> {
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

pub fn dt_to_str(d: &Option<chrono::DateTime<chrono::Utc>>) -> Option<String> {
    d.map(|x| x.to_rfc3339())
}

pub fn delete_missing(
    conn: &Connection,
    table: &str,
    column: &str,
    values: &[String],
) -> Result<()> {
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

pub fn str_to_dt(s: Option<String>) -> Option<chrono::DateTime<chrono::Utc>> {
    s.and_then(|x| {
        chrono::DateTime::parse_from_rfc3339(&x)
            .ok()
            .map(|d| d.with_timezone(&chrono::Utc))
    })
}

/// 直读（CLI 读命令 / server 启动）；WAL 下与单写者并发安全
pub fn load_state() -> Result<AppState> {
    let conn = open_db_at(&db_path())?;
    let mut st = load_state_from_conn(&conn)?;
    st.settings = crate::config::load_or_create()?;
    Ok(st)
}

/// 读取 subs.json 清单 `[{name?,url}]`，空名按序号补齐；URL 中的日期占位符按当天展开
/// （如 `v{yyyyMMdd}`；文件不存在则报错提示）
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
        .map(|(n, u)| (n, expand_date_url(&u)))
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
    fn test_loader_expands_date_placeholder() {
        // loader 返回的 URL 应已按当天展开，不再含花括号占位符
        let dir = std::env::temp_dir().join(format!("proxytool-datetpl-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("subs.json");
        std::fs::write(&p, r#"[{"name":"d","url":"https://a/v{yyyyMMdd}"}]"#).unwrap();
        let v = load_subs_config(&p).unwrap();
        let today = chrono::Local::now().format("%Y%m%d").to_string();
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].1, format!("https://a/v{today}"));
        let _ = std::fs::remove_dir_all(&dir);
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
        // 不同协议同端点：不合并（UDP 系与 TCP 系可共存同端口）
        let mut trojan = test_node("t", "s", "2.2.2.2", 443);
        trojan.r#type = NodeType::Trojan;
        upsert_nodes_conn(&conn, &[trojan]).unwrap();
        let st = load_state_from_conn(&conn).unwrap();
        assert_eq!(st.nodes.len(), 2);
        // 空 addr 互不合并
        upsert_nodes_conn(
            &conn,
            &[test_node("c", "s", "", 443), test_node("d", "s", "", 443)],
        )
        .unwrap();
        let st = load_state_from_conn(&conn).unwrap();
        assert_eq!(st.nodes.len(), 4);
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
        // 必须留下"已测"痕迹，否则 prune 会把失败节点误判成未测
        assert!(back.last_test_at.is_some());
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
