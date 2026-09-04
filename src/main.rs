mod client;
mod cmd;
mod config;
mod config_gen;
mod ctx;
mod db;
mod flow;
mod fmt;
mod ipinfo;
mod joblog;
mod model;
mod probe;
mod proxy;
mod rpc;
mod run;
mod select;
mod server;
mod store;
mod sub;
mod tester;
mod watch;

use anyhow::{anyhow, Result};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "proxytool",
    version,
    about = "高性能多协议订阅/测速/多出口 CLI (Rust, server 单写者架构)"
)]
struct Cli {
    /// 订阅清单 JSON 路径，默认 ~/.config/proxytool/subs.json，格式 [{name?,url}]
    #[arg(long, global = true)]
    subs: Option<String>,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// 订阅管理（清单来自 JSON，需显式 update 才更新节点，避免重复更新）
    Sub {
        #[command(subcommand)]
        cmd: SubCmd,
    },
    /// 测速
    Test {
        #[command(flatten)]
        opts: rpc::TestParams,
    },
    /// 去除失效
    Prune {
        #[command(flatten)]
        opts: rpc::PruneParams,
    },
    /// 本机公网 IP（对照组，直连执行不依赖 server）
    Myip {
        #[arg(long, default_value_t = 10)]
        timeout: u64,
    },
    /// 小批量真实探测，全量测完取最快（默认 www.google.com 首页）
    Probe {
        #[command(flatten)]
        opts: rpc::ProbeParams,
    },
    /// 列表（直读）
    List {
        #[arg(long, default_value = "delay")]
        sort: String,
        #[arg(long, default_value_t = false)]
        alive_only: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    /// 一键全流程：更新订阅 -> 测速 -> 去除失效 -> 真实探测 -> 启动代理 -> 常驻看护
    /// 默认提交即返回（job 在 server 内后台执行）；--follow 实时回显日志
    Auto {
        #[arg(long, default_value_t = false)]
        follow: bool,
        #[command(flatten)]
        opts: rpc::AutoParams,
    },
    /// 启动代理（单或多）
    Run {
        #[command(flatten)]
        opts: rpc::RunParams,
    },
    /// 状态（直读）
    Status {
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    /// 停止代理
    Stop {
        #[command(flatten)]
        opts: rpc::StopParams,
    },
    /// 切换节点（默认热切换，自动重启 sing-box 立即生效）
    Switch {
        #[command(flatten)]
        opts: rpc::SwitchParams,
    },
    /// 导出（直读）
    Export {
        #[arg(long, default_value = "uri")]
        format: String,
        #[arg(long, default_value_t = false)]
        alive_only: bool,
        #[arg(long)]
        output: Option<String>,
    },
    /// 补查 IP
    Ipinfo {
        #[command(flatten)]
        opts: rpc::IpinfoParams,
    },
    /// 常驻 server（单写者；所有写命令经它执行，CLI 缺 server 时自动拉起）
    Serve {
        /// 前台运行（调试用；默认后台分叉立即返回）
        #[arg(long, default_value_t = false)]
        foreground: bool,
        /// 停止 server（含其托管的所有代理与看护）
        #[arg(long, default_value_t = false)]
        stop: bool,
    },
}

#[derive(Subcommand)]
enum SubCmd {
    /// 仅展示 JSON 清单（不抓取）
    List,
    /// 抓取并更新节点（解析后自动去重）
    Update {
        #[arg(long)]
        name: Option<String>,
    },
}

fn resolve_subs_path(cli_subs: &Option<String>) -> PathBuf {
    if let Some(p) = cli_subs {
        PathBuf::from(p)
    } else {
        store::default_subs_path()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let cli = Cli::parse();
    // 更名迁移：v2ray-cli -> proxytool（幂等）
    store::migrate_legacy_dirs();
    let subs_abs = resolve_subs_path(&cli.subs).to_string_lossy().to_string();
    match cli.command {
        Commands::Serve {
            foreground,
            stop,
        } => server::serve(foreground, stop, cli.subs.clone()).await?,
        Commands::Sub { cmd } => match cmd {
            SubCmd::List => handle_sub_list(&cli.subs)?,
            SubCmd::Update { name } => {
                client::run_job(
                    "sub.update",
                    serde_json::to_value(rpc::SubUpdateParams {
                        name,
                        subs: Some(subs_abs),
                    })?,
                    true,
                )
                .await?
            }
        },
        Commands::Test { opts } => client::run_job("test", serde_json::to_value(opts)?, true).await?,
        Commands::Prune { opts } => {
            client::run_job("prune", serde_json::to_value(opts)?, true).await?
        }
        Commands::Myip { timeout } => handle_myip(timeout).await?,
        Commands::Probe { opts } => {
            client::run_job("probe", serde_json::to_value(opts)?, true).await?
        }
        Commands::List {
            sort,
            alive_only,
            json,
        } => handle_list(sort, alive_only, json)?,
        Commands::Auto { follow, mut opts } => {
            opts.subs = Some(subs_abs);
            client::run_job("auto", serde_json::to_value(&opts)?, follow).await?
        }
        Commands::Run { opts } => client::run_job("run", serde_json::to_value(opts)?, true).await?,
        Commands::Status { json } => handle_status(json)?,
        Commands::Stop { opts } => client::run_job("stop", serde_json::to_value(opts)?, true).await?,
        Commands::Switch { opts } => {
            client::run_job("switch", serde_json::to_value(opts)?, true).await?
        }
        Commands::Export {
            format,
            alive_only,
            output,
        } => handle_export(format, alive_only, output)?,
        Commands::Ipinfo { opts } => {
            client::run_job("ipinfo", serde_json::to_value(opts)?, true).await?
        }
    }
    Ok(())
}

fn handle_sub_list(cli_subs: &Option<String>) -> Result<()> {
    let subs = store::load_subs_config(&resolve_subs_path(cli_subs))?;
    let st = store::load_state()?;
    for (name, url) in subs {
        let updated = st
            .subs
            .iter()
            .find(|s| s.name == name)
            .and_then(|s| s.updated_at);
        println!("{name} -> {url} updated:{updated:?}");
    }
    Ok(())
}

async fn handle_myip(timeout: u64) -> Result<()> {
    let st = store::load_state()?;
    let api = st.settings.ip_api_url.clone();
    println!("查询本机公网 IP -> {api}");
    match ipinfo::fetch_my_ip(&api, timeout).await {
        Ok((ip, cc)) => {
            println!("本机: ip={ip} cc={cc}");
            if let Ok(txt) = reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(timeout))
                .build()?
                .get(&api)
                .send()
                .await?
                .text()
                .await
            {
                println!("原始: {txt:.500}");
            }
        }
        Err(e) => return Err(anyhow!("获取本机 IP 失败: {e}")),
    }
    Ok(())
}

fn handle_list(sort: String, alive_only: bool, json: bool) -> Result<()> {
    let st = store::load_state()?;
    let mut nodes = st.nodes.clone();
    if alive_only {
        nodes.retain(|n| n.alive);
    }
    match sort.as_str() {
        "delay" => nodes.sort_by_key(|n| if n.delay_ms > 0 { n.delay_ms } else { 99999 }),
        "speed" => nodes.sort_by(|a, b| {
            let sa = a.speed_kbps.unwrap_or(0.0);
            let sb = b.speed_kbps.unwrap_or(0.0);
            sb.partial_cmp(&sa).unwrap_or(std::cmp::Ordering::Equal)
        }),
        "cc" => nodes.sort_by(|a, b| a.cc.cmp(&b.cc)),
        _ => select::sort_nodes_by_delay(&mut nodes),
    }
    if json {
        println!("{}", serde_json::to_string_pretty(&nodes)?);
    } else {
        for (i, n) in nodes.iter().take(100).enumerate() {
            println!(
                "{:3} [{}] {:6} {:5}ms {:7.1}KB/s {:3} {:15}:{} ip={} cc={}",
                i,
                n.sub,
                n.r#type.as_str(),
                n.delay_ms,
                n.speed_kbps.unwrap_or(0.0),
                if n.alive { "OK" } else { "--" },
                n.addr,
                n.port,
                n.exit_ip.as_deref().unwrap_or("-"),
                n.cc.as_deref().unwrap_or("-")
            );
        }
        if nodes.len() > 100 {
            println!("... 共 {} 节点 仅显示前 100", nodes.len());
        }
    }
    Ok(())
}

fn handle_status(json: bool) -> Result<()> {
    let st = store::load_state()?;
    if json {
        println!("{}", serde_json::to_string_pretty(&st)?);
        return Ok(());
    }
    match st
        .meta
        .get("server.pid")
        .and_then(|v| v.parse::<u32>().ok())
    {
        Some(pid) if run::is_tool_alive(pid) => println!("server pid={pid} 运行中"),
        Some(pid) => println!("server pid={pid} 已退出（写命令将自动拉起）"),
        None => println!("server 未运行（写命令将自动拉起）"),
    }
    let mut ports: Vec<u16> = st.running.iter().map(|r| r.port).collect();
    ports.sort_unstable();
    println!(
        "共 {} 节点 存活 {} 端口 {:?}",
        st.nodes.len(),
        st.nodes.iter().filter(|n| n.alive).count(),
        ports
    );
    if st.running.is_empty() {
        println!("无运行中代理，请先 run/auto");
    }
    for r in &st.running {
        println!("{}", select::describe_running(&st, r));
    }
    Ok(())
}

fn handle_export(format: String, alive_only: bool, output: Option<String>) -> Result<()> {
    let st = store::load_state()?;
    let mut nodes = st.nodes.clone();
    if alive_only {
        nodes.retain(|n| n.alive);
    }
    let out = match format.as_str() {
        "json" => serde_json::to_string_pretty(&nodes)?,
        "uri" => nodes
            .iter()
            .map(|n| n.cred.clone())
            .collect::<Vec<_>>()
            .join("\n"),
        other => return Err(anyhow!("不支持的导出格式: {other}（仅支持 uri/json）")),
    };
    if let Some(p) = output {
        std::fs::write(&p, &out)?;
        println!("已导出到 {p}");
    } else {
        println!("{out}");
    }
    Ok(())
}

#[cfg(test)]
mod cli_tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn test_auto_parses_with_defaults() {
        let cli = Cli::try_parse_from(["proxytool", "auto"]).unwrap();
        match cli.command {
            Commands::Auto { follow, opts } => {
                assert!(!follow);
                assert_eq!(opts.port, 10808);
                assert_eq!(opts.batch_size, 15);
                assert_eq!(opts.max_batches, 60);
                assert!(!opts.no_daemon);
                assert!(!opts.skip_update);
            }
            _ => panic!("auto 解析到错误子命令"),
        }
    }

    #[test]
    fn test_auto_parses_overrides() {
        let cli = Cli::try_parse_from([
            "proxytool",
            "auto",
            "--port",
            "18282",
            "--filter",
            "HK",
            "--skip-test",
            "--follow",
        ])
        .unwrap();
        match cli.command {
            Commands::Auto { follow, opts } => {
                assert!(follow);
                assert_eq!(opts.port, 18282);
                assert_eq!(opts.filter.as_deref(), Some("HK"));
                assert!(opts.skip_test);
            }
            _ => panic!("auto 解析到错误子命令"),
        }
    }

    #[test]
    fn test_run_parses_multi_ports() {
        let cli =
            Cli::try_parse_from(["proxytool", "run", "--ports", "10808,10809", "--distinct-cc"])
                .unwrap();
        match cli.command {
            Commands::Run { opts } => {
                assert_eq!(opts.ports.as_deref(), Some("10808,10809"));
                assert!(opts.distinct_cc);
            }
            _ => panic!("run 解析到错误子命令"),
        }
    }

    #[test]
    fn test_serve_flags() {
        let cli = Cli::try_parse_from(["proxytool", "serve"]).unwrap();
        match cli.command {
            Commands::Serve { foreground, stop } => {
                // 默认后台
                assert!(!foreground);
                assert!(!stop);
            }
            _ => panic!("serve 解析到错误子命令"),
        }
        let cli = Cli::try_parse_from(["proxytool", "serve", "--foreground"]).unwrap();
        match cli.command {
            Commands::Serve { foreground, stop } => {
                assert!(foreground && !stop);
            }
            _ => panic!("serve 解析到错误子命令"),
        }
    }
}
