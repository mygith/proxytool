use anyhow::{Result, anyhow};
use base64::{Engine as _, engine::general_purpose::STANDARD as B64};

use crate::{fmt, model::Node};

pub async fn fetch_subscription(url: &str) -> Result<String> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .user_agent("proxytool/0.1")
        .build()?;
    let resp = client.get(url).send().await?;
    if !resp.status().is_success() {
        return Err(anyhow!("http {}", resp.status()));
    }
    let text = resp.text().await?;
    Ok(text)
}

fn try_b64_decode(s: &str) -> Option<String> {
    let t = s.trim().replace('-', "+").replace('_', "/");
    let mut padded = t.clone();
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

pub fn endpoint_key(addr: &str, port: u16) -> Option<String> {
    let a = addr.trim().to_ascii_lowercase();
    if a.is_empty() || port == 0 {
        return None;
    }
    Some(format!("{a}:{port}"))
}

/// 按 ip:port 去重，保留首次出现顺序；addr 为空或 port==0 则按 cred 兜底不合并
pub fn dedup_by_endpoint(nodes: Vec<Node>) -> (Vec<Node>, usize) {
    use std::collections::HashSet;
    let total = nodes.len();
    let mut seen = HashSet::new();
    let mut out = Vec::with_capacity(total);
    for n in nodes {
        let key = match endpoint_key(&n.addr, n.port) {
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
    let mut candidates: Vec<String> = Vec::new();

    let lines: Vec<String> = raw
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty() && !l.starts_with('#') || l.contains("://"))
        .filter(|l| l.contains("://"))
        .collect();

    if !lines.is_empty() {
        candidates = lines;
    } else {
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
    use std::collections::HashSet;
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
        println!("已过滤畸形节点 {} 个（内网地址/端口 0）", before_bogus - nodes.len());
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
    fn test_dedup_by_endpoint_same_ip_port() {
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
}
