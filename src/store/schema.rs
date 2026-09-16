use anyhow::Result;
use rusqlite::Connection;

const SCHEMA_VERSION: i32 = 3;

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
        -- 端点唯一按 协议-host:port（UDP 系与 TCP 系可共存同端口，不得合并；
        -- 空 addr/0 端口不参与，沿用内存去重兜底语义）
        DROP INDEX IF EXISTS idx_nodes_endpoint;
        CREATE UNIQUE INDEX idx_nodes_endpoint
            ON nodes(proto, lower(addr), port) WHERE addr != '' AND port != 0;
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
        .is_ok_and(|mut q| q.exists([]).unwrap_or(false));
    if !has_probed {
        conn.execute_batch("ALTER TABLE nodes ADD COLUMN probed INTEGER NOT NULL DEFAULT 0;")?;
    }
    Ok(())
}
