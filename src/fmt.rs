use anyhow::{Result, anyhow};
use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
use serde_json::Value;
use url::Url;

use crate::model::{Node, NodeType};

fn md5_id(s: &str) -> String {
    format!("{:x}", md5::compute(s))
}

fn legacy_vmess_query(v: &Value) -> Option<String> {
    let mut pairs = Vec::new();
    let push = |pairs: &mut Vec<String>, key: &str, value: Option<&str>| {
        if let Some(value) = value.filter(|value| !value.is_empty()) {
            pairs.push(format!("{}={}", key, urlencoding::encode(value)));
        }
    };

    let net = v.get("net").and_then(Value::as_str).unwrap_or_default();
    if !net.is_empty() {
        let net_lower = net.to_ascii_lowercase();
        let transport = match net_lower.as_str() {
            "websocket" => "ws",
            "httpupgrade" => "http",
            other => other,
        };
        push(&mut pairs, "type", Some(transport));
    }
    if let Some(tls) = v.get("tls").and_then(Value::as_str)
        && !tls.is_empty()
        && !tls.eq_ignore_ascii_case("none")
    {
        push(&mut pairs, "security", Some("tls"));
    }
    for key in ["host", "path", "sni", "alpn", "pbk", "sid", "fp"] {
        push(&mut pairs, key, v.get(key).and_then(Value::as_str));
    }
    (!pairs.is_empty()).then(|| pairs.join("&"))
}

fn decode_b64_maybe(s: &str) -> Option<String> {
    let mut padded = s.trim().to_string();
    padded = padded.replace('-', "+").replace('_', "/");
    let rem = padded.len() % 4;
    if rem != 0 {
        padded.push_str(&"=".repeat(4 - rem));
    }
    B64.decode(padded)
        .ok()
        .and_then(|b| String::from_utf8(b).ok())
}

/// 解析单行 uri 为精简 Node（不存名称/原始查询缓存，cred 存完整行）
pub fn parse_uri(line: &str, sub: &str) -> Result<Node> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return Err(anyhow!("empty/comment"));
    }
    if line.starts_with("vmess://") {
        return parse_vmess(line, sub);
    }
    if line.starts_with("ss://")
        && let Ok(n) = parse_shadowsocks(line, sub)
    {
        return Ok(n);
    }
    let url = Url::parse(line).map_err(|e| anyhow!("url parse {e}: {line}"))?;
    let scheme = url.scheme().to_ascii_lowercase();
    let node_type = NodeType::from_scheme(&scheme);
    let addr = url.host_str().unwrap_or("").to_string();
    let port = url.port().unwrap_or(0);
    let mut n = Node::new(sub, node_type, &addr, port, line);
    // id 保持 md5(cred)，与旧逻辑一致以便合并保留测速
    n.id = md5_id(line);
    Ok(n)
}

/// 兼容旧调用：sub 为空
#[allow(dead_code)]
pub fn parse_uri_legacy(line: &str) -> Result<Node> {
    parse_uri(line, "")
}

fn parse_vmess(line: &str, sub: &str) -> Result<Node> {
    let b64 = line.strip_prefix("vmess://").unwrap_or(line).trim();
    if let Some(json_str) = decode_b64_maybe(b64)
        && let Ok(v) = serde_json::from_str::<Value>(&json_str)
    {
        let addr = v
            .get("add")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        let port = v
            .get("port")
            .and_then(|x| x.as_str().or(x.as_u64().map(|_| "")))
            .and_then(|s| s.parse::<u16>().ok())
            .or_else(|| v.get("port").and_then(|x| x.as_u64()).map(|n| n as u16))
            .unwrap_or(0);
        let mut n = Node::new(sub, NodeType::Vmess, &addr, port, line);
        n.id = md5_id(line);
        return Ok(n);
    }
    let url = Url::parse(line).map_err(|e| anyhow!("vmess url {e}"))?;
    let addr = url.host_str().unwrap_or("").to_string();
    let port = url.port().unwrap_or(0);
    let mut n = Node::new(sub, NodeType::Vmess, &addr, port, line);
    n.id = md5_id(line);
    Ok(n)
}

fn parse_shadowsocks(line: &str, sub: &str) -> Result<Node> {
    let url = Url::parse(line).map_err(|e| anyhow!("ss url {e}"))?;
    let mut addr = url.host_str().unwrap_or("").to_string();
    let mut port = url.port().unwrap_or(0);

    if addr.is_empty() {
        let after = line.strip_prefix("ss://").unwrap_or(line);
        let without_frag = after.split('#').next().unwrap_or("");
        let without_query = without_frag.split('?').next().unwrap_or("");
        if let Some(decoded) = decode_b64_maybe(without_query)
            && let Ok(u) = Url::parse(&format!("ss://{decoded}"))
        {
            addr = u.host_str().unwrap_or("").to_string();
            port = u.port().unwrap_or(port);
        }
    }

    let mut n = Node::new(sub, NodeType::Shadowsocks, &addr, port, line);
    n.id = md5_id(line);
    Ok(n)
}

/// 供 config_gen 按需从 cred 反解析连接信息（仅 query/userinfo；地址与协议以 Node 字段为准）
pub struct CredInfo {
    pub query: Option<String>,
    pub userinfo: Option<String>,
}

pub fn parse_cred(cred: &str) -> Result<CredInfo> {
    let line = cred.trim();
    // vmess 旧 base64 json
    if line.starts_with("vmess://") {
        let b64 = line.strip_prefix("vmess://").unwrap_or(line).trim();
        // 去掉 # 备注后再解码
        let b64_core = b64.split('#').next().unwrap_or("");
        if let Some(js) = decode_b64_maybe(b64_core)
            && let Ok(v) = serde_json::from_str::<Value>(&js)
        {
            // vmess JSON 的 id 字段即 uuid
            let userinfo = v.get("id").and_then(|x| x.as_str()).map(|s| s.to_string());
            return Ok(CredInfo {
                query: legacy_vmess_query(&v),
                userinfo,
            });
        }
    }
    // ss 需解码 userinfo 中的 method:pass
    if line.starts_with("ss://")
        && let Ok(url) = Url::parse(line)
    {
        let mut method_pass: Option<String> = None;
        let username = url.username().to_string();
        if !username.is_empty() && !username.contains(':') {
            if let Some(d) = decode_b64_maybe(&username) {
                method_pass = Some(d);
            }
        } else if !username.is_empty() {
            method_pass = Some(username);
        }
        if method_pass.is_none() {
            // 整体 base64 形式的 ss 链接，从解码后 URL 补取 method:pass
            let after = line.strip_prefix("ss://").unwrap_or(line);
            let without_frag = after.split('#').next().unwrap_or("");
            let without_query = without_frag.split('?').next().unwrap_or("");
            if let Some(decoded) = decode_b64_maybe(without_query)
                && let Ok(u) = Url::parse(&format!("ss://{decoded}"))
            {
                let du = u.username();
                if !du.is_empty() {
                    method_pass = Some(du.to_string());
                }
            }
        }
        return Ok(CredInfo {
            query: url.query().map(|s| s.to_string()),
            userinfo: method_pass,
        });
    }
    let url = Url::parse(line).map_err(|e| anyhow!("url parse {e}"))?;
    let userinfo = if url.username().is_empty() && url.password().is_none() {
        None
    } else {
        Some(format!(
            "{}{}",
            url.username(),
            url.password().map(|p| format!(":{p}")).unwrap_or_default()
        ))
    };
    Ok(CredInfo {
        query: url.query().map(|s| s.to_string()),
        userinfo,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_vless() {
        let n = parse_uri(
            "vless://uuid@1.1.1.1:443?security=reality&sni=example.com#test",
            "1",
        )
        .unwrap();
        assert_eq!(n.r#type, NodeType::Vless);
        assert_eq!(n.addr, "1.1.1.1");
        assert_eq!(n.port, 443);
        assert_eq!(n.sub, "1");
        assert!(n.cred.starts_with("vless://"));
    }
    #[test]
    fn test_hy2() {
        let n = parse_uri(
            "hysteria2://pass@1.1.1.1:443/?insecure=1&sni=example.com#hy",
            "s",
        )
        .unwrap();
        assert_eq!(n.r#type, NodeType::Hysteria2);
    }
    #[test]
    fn test_ss() {
        let n = parse_uri(
            "ss://Y2hhY2hhMjAtaWV0Zi1wb2x5MTMwNTpwYXNz@1.1.1.1:8388#ss-test",
            "1",
        )
        .unwrap();
        assert_eq!(n.r#type, NodeType::Shadowsocks);
        let info = parse_cred(&n.cred).unwrap();
        assert_eq!(
            info.userinfo.as_deref(),
            Some("chacha20-ietf-poly1305:pass")
        );
    }
    #[test]
    fn test_parse_cred_vless_query() {
        let info = parse_cred("vless://u@1.1.1.1:443?security=tls&type=ws&host=e.com&path=%2Fws#t")
            .unwrap();
        assert_eq!(info.userinfo.as_deref(), Some("u"));
        assert!(info.query.unwrap().contains("type=ws"));
    }

    #[test]
    fn test_legacy_vmess_keeps_transport_and_tls_parameters() {
        let payload = serde_json::json!({
            "add": "1.1.1.1",
            "port": "443",
            "id": "11111111-1111-1111-1111-111111111111",
            "net": "ws",
            "host": "cdn.example",
            "path": "/ws",
            "tls": "tls"
        });
        let encoded = base64::engine::general_purpose::STANDARD
            .encode(serde_json::to_string(&payload).unwrap());
        let n = parse_uri(&format!("vmess://{encoded}"), "1").unwrap();
        let info = parse_cred(&n.cred).unwrap();
        assert!(
            info.query
                .as_deref()
                .unwrap_or_default()
                .contains("type=ws")
        );
        assert!(
            info.query
                .as_deref()
                .unwrap_or_default()
                .contains("security=tls")
        );
    }
}
