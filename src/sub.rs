use anyhow::{Result, anyhow, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
use std::time::Duration;

use crate::{fmt, model::Node, say, store};

/// 抓取订阅原文。git+ 前缀走 git（如 git+ssh://git@host/u/r.git#main:path），其余走 HTTP
pub async fn fetch_subscription(url: &str) -> Result<String> {
    if let Some(spec) = url.strip_prefix("git+") {
        return fetch_git_spec(spec).await;
    }
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .user_agent("proxytool/0.1")
        .build()?;
    let resp = client.get(url).send().await?;
    if !resp.status().is_success() {
        return Err(anyhow!("http {}", resp.status()));
    }
    let text = resp.text().await?;
    Ok(text)
}

/// 拆分 git 订阅地址：ssh://host/u/r.git#<ref>:<path>，ref 留空表示远端默认分支
fn parse_git_spec(spec: &str) -> Result<(&str, Option<&str>, &str)> {
    let (repo, frag) = spec
        .split_once('#')
        .ok_or_else(|| anyhow!("git 订阅缺少 #<ref>:<path> 片段: {spec}"))?;
    let (git_ref, path) = frag
        .split_once(':')
        .ok_or_else(|| anyhow!("git 订阅片段应形如 <ref>:<path>（ref 可留空）: {spec}"))?;
    let path = path.trim_start_matches('/');
    if path.is_empty() {
        bail!("git 订阅缺少文件路径: {spec}");
    }
    Ok((repo, (!git_ref.is_empty()).then_some(git_ref), path))
}

/// 缓存目录名：去掉协议、user@ 与 .git 后缀，其余非法字符换成 _
fn git_cache_key(repo: &str) -> String {
    let s = repo.split_once("://").map_or(repo, |(_, rest)| rest);
    let s = s.split_once('@').map_or(s, |(_, host)| host);
    let s = s.strip_suffix(".git").unwrap_or(s);
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() || matches!(c, '.' | '-') { c } else { '_' })
        .collect()
}

/// 本地缓存仓库：只拉目标 ref 的树（blob:none），再按需取单个文件
async fn fetch_git_spec(spec: &str) -> Result<String> {
    let (repo, git_ref, path) = parse_git_spec(spec)?;
    let dir = store::data_dir().join("git-cache").join(git_cache_key(repo));
    std::fs::create_dir_all(&dir)?;
    let dir_s = dir.to_string_lossy().into_owned();
    // init 幂等；直接对 URL fetch，无需维护 remote
    run_git(&["init", "--quiet", dir_s.as_str()]).await?;
    let rev = git_ref.unwrap_or("HEAD");
    run_git(&[
        "-C", dir_s.as_str(), "fetch", "--quiet", "--depth", "1", "--filter=blob:none", repo, rev,
    ])
    .await?;
    let object = format!("FETCH_HEAD:{path}");
    let raw = run_git(&["-C", dir_s.as_str(), "show", object.as_str()]).await?;
    // 旧 commit 已无引用，回收其 blob，避免缓存随每次更新无限增长；失败不影响本次结果
    let _ = run_git(&["-C", dir_s.as_str(), "gc", "--prune=now", "--quiet"]).await;
    String::from_utf8(raw).map_err(|e| anyhow!("订阅内容不是合法 UTF-8: {e}"))
}

/// 执行 git 并返回 stdout；失败时带 stderr，便于定位 SSH/权限/路径问题
async fn run_git(args: &[&str]) -> Result<Vec<u8>> {
    let out = tokio::process::Command::new("git")
        .args(args)
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .await
        .map_err(|e| anyhow!("无法执行 git（{e}）：git 订阅需本机安装 git 并配好 SSH key"))?;
    if !out.status.success() {
        return Err(anyhow!(
            "git {} 失败: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    Ok(out.stdout)
}

fn try_b64_decode(s: &str) -> Option<String> {
    let t = s.trim().replace('-', "+").replace('_', "/");
    let mut padded = t;
    let rem = padded.len() % 4;
    if rem != 0 {
        padded.push_str(&"=".repeat(4 - rem));
    }
    B64.decode(padded)
        .ok()
        .and_then(|b| String::from_utf8(b).ok())
}

#[derive(Debug, Clone, Default)]
pub struct DedupStats {
    pub raw: usize,
    pub uri_unique: usize,
    pub endpoint_unique: usize,
}

/// 端点 key：协议-lower(addr)-port（UDP 系 hysteria2/tuic 与 TCP 系可共存同端口，不能按 ip:port 合并）
fn endpoint_key(n: &Node) -> Option<String> {
    let a = n.addr.trim().to_ascii_lowercase();
    if a.is_empty() || n.port == 0 {
        return None;
    }
    Some(format!("{}-{a}:{}", n.r#type.as_str(), n.port))
}

/// 按 协议-host:port 去重，保留首次出现顺序；addr 为空或 port==0 则按 cred 兜底不合并
pub fn dedup_by_endpoint(nodes: Vec<Node>) -> (Vec<Node>, usize) {
    use std::collections::HashSet;
    let total = nodes.len();
    let mut seen = HashSet::new();
    let mut out = Vec::with_capacity(total);
    for n in nodes {
        let key = match endpoint_key(&n) {
            Some(k) => k,
            None => format!("uri:{}", n.cred),
        };
        if seen.insert(key) {
            out.push(n);
        }
    }
    let removed = total.saturating_sub(out.len());
    (out, removed)
}

/// 解析订阅内容并自动去重（uri 去重 + ip:port 去重），调用方无需再去重
pub fn parse_subscription_content(raw: &str, sub: &str) -> (Vec<Node>, DedupStats) {
    use std::collections::HashSet;
    let mut candidates: Vec<String> = Vec::new();

    let lines: Vec<String> = raw
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty() && !l.starts_with('#') || l.contains("://"))
        .filter(|l| l.contains("://"))
        .collect();

    if lines.is_empty() {
        let trimmed = raw.trim().replace(|c: char| c.is_whitespace(), "");
        if let Some(decoded) = try_b64_decode(&trimmed) {
            let ls: Vec<String> = decoded
                .lines()
                .map(|l| l.trim().to_string())
                .filter(|l| l.contains("://"))
                .collect();
            if !ls.is_empty() {
                candidates = ls;
            }
        }
    } else {
        candidates = lines;
    }

    if candidates.is_empty() {
        let no_ws = raw.replace(|c: char| c.is_whitespace(), "");
        if let Some(dec) = try_b64_decode(&no_ws) {
            candidates = dec
                .lines()
                .map(|l| l.trim().to_string())
                .filter(|l| l.contains("://"))
                .collect();
        }
    }

    let raw_count = candidates.len();
    // uri 去重
    let mut seen_uri = HashSet::new();
    let mut nodes = Vec::new();
    for line in candidates {
        if !seen_uri.insert(line.clone()) {
            continue;
        }
        match fmt::parse_uri(&line, sub) {
            Ok(n) => nodes.push(n),
            Err(_) => {
                if let Some(dec) = try_b64_decode(&line) {
                    let dec = dec.trim().to_string();
                    if seen_uri.insert(dec.clone())
                        && let Ok(n) = fmt::parse_uri(&dec, sub)
                    {
                        nodes.push(n);
                    }
                }
            }
        }
    }
    let uri_unique = nodes.len();
    let (nodes, _) = dedup_by_endpoint(nodes);
    // 入口过滤：内网/保留地址（127.x/192.168.x 等）、端口 0 的畸形节点直接丢弃
    let before_bogus = nodes.len();
    let nodes: Vec<Node> = nodes
        .into_iter()
        .filter(|n| !crate::config::is_bogus_endpoint(&n.addr, n.port))
        .collect();
    if nodes.len() < before_bogus {
        say!("已过滤畸形节点 {} 个（内网地址/端口 0）", before_bogus - nodes.len());
    }
    let stats = DedupStats {
        raw: raw_count,
        uri_unique,
        endpoint_unique: nodes.len(),
    };
    (nodes, stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::NodeType;

    fn mk(addr: &str, port: u16, cred: &str) -> Node {
        Node::new("1", NodeType::Vless, addr, port, cred)
    }

    #[test]
    fn test_dedup_by_endpoint_same_proto_host_port() {
        let nodes = vec![
            mk("1.1.1.1", 443, "vless://a@1.1.1.1:443#1"),
            mk("1.1.1.1", 443, "vless://b@1.1.1.1:443#2"),
            mk("1.1.1.1", 8443, "vless://c@1.1.1.1:8443#3"),
        ];
        let (out, removed) = dedup_by_endpoint(nodes);
        assert_eq!(out.len(), 2);
        assert_eq!(removed, 1);
        assert_eq!(out[0].cred, "vless://a@1.1.1.1:443#1");
    }

    #[test]
    fn test_dedup_by_endpoint_keeps_cross_proto_same_port() {
        // UDP 系与 TCP 系共存同端口：不得合并
        let nodes = vec![
            mk("1.1.1.1", 443, "vless://a@1.1.1.1:443#1"),
            Node::new("1", NodeType::Trojan, "1.1.1.1", 443, "trojan://b@1.1.1.1:443#2"),
            Node::new("1", NodeType::Hysteria2, "1.1.1.1", 443, "hysteria2://c@1.1.1.1:443#3"),
        ];
        let (out, removed) = dedup_by_endpoint(nodes);
        assert_eq!(out.len(), 3);
        assert_eq!(removed, 0);
    }

    #[test]
    fn test_dedup_by_endpoint_case_insensitive() {
        let nodes = vec![mk("Example.COM", 443, "u1"), mk("example.com", 443, "u2")];
        let (out, removed) = dedup_by_endpoint(nodes);
        assert_eq!(out.len(), 1);
        assert_eq!(removed, 1);
    }

    #[test]
    fn test_dedup_by_endpoint_empty_not_merged() {
        let nodes = vec![
            mk("", 443, "u1"),
            mk("", 443, "u2"),
            mk("1.1.1.1", 0, "u3"),
            mk("1.1.1.1", 0, "u4"),
        ];
        let (out, removed) = dedup_by_endpoint(nodes);
        assert_eq!(out.len(), 4);
        assert_eq!(removed, 0);
    }

    #[test]
    fn test_parse_auto_dedup() {
        let raw = "vless://a@1.1.1.1:443#1\nvless://b@1.1.1.1:443#2\nvless://c@1.1.1.1:8443#3\n";
        let (nodes, stats) = parse_subscription_content(raw, "1");
        assert_eq!(stats.raw, 3);
        assert_eq!(nodes.len(), 2);
        assert_eq!(stats.endpoint_unique, 2);
        assert!(nodes.iter().all(|n| n.sub == "1"));
    }

    #[test]
    fn test_parse_assigns_sub() {
        let raw = "vless://u@2.2.2.2:443#x\n";
        let (nodes, _) = parse_subscription_content(raw, "my");
        assert_eq!(nodes[0].sub, "my");
    }

    #[test]
    fn test_parse_git_spec_with_ref() {
        let (repo, git_ref, path) =
            parse_git_spec("ssh://git@github.com/u/r.git#main:dir/sub.txt").unwrap();
        assert_eq!(repo, "ssh://git@github.com/u/r.git");
        assert_eq!(git_ref, Some("main"));
        assert_eq!(path, "dir/sub.txt");
    }

    #[test]
    fn test_parse_git_spec_empty_ref_means_default_branch() {
        let (_, git_ref, path) = parse_git_spec("ssh://git@github.com/u/r.git#:/sub.txt").unwrap();
        assert_eq!(git_ref, None);
        assert_eq!(path, "sub.txt");
    }

    #[test]
    fn test_parse_git_spec_rejects_missing_parts() {
        assert!(parse_git_spec("ssh://git@github.com/u/r.git").is_err());
        assert!(parse_git_spec("ssh://git@github.com/u/r.git#main:").is_err());
        assert!(parse_git_spec("ssh://git@github.com/u/r.git#main").is_err());
    }

    #[test]
    fn test_git_cache_key_strips_scheme_user_and_dot_git() {
        assert_eq!(git_cache_key("ssh://git@github.com/u/r.git"), "github.com_u_r");
        assert_eq!(git_cache_key("ssh://github.com:22/u/r"), "github.com_22_u_r");
    }
}
