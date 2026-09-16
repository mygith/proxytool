use anyhow::{Result, anyhow};
use serde_json::{Map, Value, json};
use std::collections::HashMap;
use std::net::IpAddr;

use crate::{fmt, model::Node};

fn parse_query_map(q: Option<&str>) -> HashMap<String, String> {
    let mut m = HashMap::new();
    if let Some(q) = q {
        for kv in q.split('&') {
            if kv.is_empty() {
                continue;
            }
            let mut it = kv.splitn(2, '=');
            let k = it.next().unwrap_or("").to_ascii_lowercase();
            let v_enc = it.next().unwrap_or("");
            let v = urlencoding::decode(v_enc).unwrap_or_default().to_string();
            m.insert(k, v);
        }
    }
    m
}

fn build_transport(qm: &HashMap<String, String>) -> Value {
    let typ = qm
        .get("type").map_or_else(|| "tcp".to_string(), |s| s.to_ascii_lowercase());
    match typ.as_str() {
        "ws" | "websocket" => {
            let path = qm.get("path").cloned().unwrap_or_else(|| "/".to_string());
            let host = qm
                .get("host")
                .or_else(|| qm.get("sni"))
                .cloned()
                .unwrap_or_default();
            if host.is_empty() {
                json!({"type": "ws", "path": path})
            } else {
                json!({"type": "ws", "path": path, "headers": {"Host": host}})
            }
        }
        "grpc" => {
            let svc = qm
                .get("servicename")
                .or_else(|| qm.get("serviceName"))
                .or_else(|| qm.get("path"))
                .cloned()
                .unwrap_or_default();
            json!({"type": "grpc", "service_name": svc})
        }
        "http" | "xhttp" | "h2" => {
            let path = qm.get("path").cloned().unwrap_or_else(|| "/".to_string());
            let host = qm.get("host").cloned().unwrap_or_default();
            json!({"type": "http", "path": path, "host": if host.is_empty() { json!([]) } else { json!([host]) }})
        }
        _ => json!({}),
    }
}

/// reality 客户端缺失 fp 时使用的 uTLS 指纹（sing-box 强制要求 utls，缺则启动失败）
const REALITY_DEFAULT_FP: &str = "chrome";

fn build_tls(
    qm: &HashMap<String, String>,
    default_sni: &str,
) -> Value {
    let sec = qm
        .get("security")
        .map(|s| s.to_ascii_lowercase())
        .unwrap_or_default();
    let sni = qm.get("sni").map_or(default_sni, String::as_str);
    let pbk = qm.get("pbk").map_or("", String::as_str);
    let sid = qm.get("sid").map_or("", String::as_str);
    let fp = qm.get("fp").map_or("", String::as_str);
    let alpn = qm.get("alpn").map_or("", String::as_str);
    let insecure = qm
        .get("insecure")
        .is_some_and(|s| s == "1" || s.eq_ignore_ascii_case("true"))
        || qm
            .get("allowinsecure")
            .is_some_and(|s| s == "1" || s.eq_ignore_ascii_case("true"));
    let need_tls =
        sec == "tls" || sec == "reality" || !pbk.is_empty() || (!sni.is_empty() && sec != "none");
    if !need_tls {
        return json!({});
    }
    let mut tls = Map::new();
    tls.insert("enabled".to_string(), json!(true));
    if !sni.is_empty() {
        tls.insert("server_name".to_string(), json!(sni));
    }
    if insecure {
        tls.insert("insecure".to_string(), json!(true));
    }
    if !alpn.is_empty() {
        let v: Vec<&str> = alpn
            .split(',')
            .map(str::trim)
            .filter(|x| !x.is_empty())
            .collect();
        if !v.is_empty() {
            tls.insert("alpn".to_string(), json!(v));
        }
    }
    let utls = |fp: &str| json!({"enabled": true, "fingerprint": fp});
    if !pbk.is_empty() {
        tls.insert(
            "reality".to_string(),
            json!({"enabled": true, "public_key": pbk, "short_id": sid}),
        );
        // sing-box 强制要求 reality 客户端带 uTLS：URI 缺 fp（常见）时补默认指纹，
        // 否则 sing-box 启动即 FATAL。xray/v2ray-core 自带默认指纹，故同一节点 v2rayN 能连
        tls.insert(
            "utls".to_string(),
            if fp.is_empty() { utls(REALITY_DEFAULT_FP) } else { utls(fp) },
        );
    } else if !fp.is_empty() {
        tls.insert("utls".to_string(), utls(fp));
    }
    Value::Object(tls)
}

#[allow(clippy::too_many_lines, reason = "每种协议有独立的 outbound 构建逻辑，拆分增加跳转成本")]
pub fn node_to_singbox_outbound(node: &Node) -> Result<Value> {
    // 按需从 cred 反解析，不依赖存储的 raw_*（精简存储）
    let info = fmt::parse_cred(&node.cred)
        .map_err(|error| anyhow!("解析节点凭证失败 {}: {error}", node.cred))?;
    match node.r#type {
        crate::model::NodeType::Vless => {
            let qm = parse_query_map(info.query.as_deref());
            let flow = qm.get("flow").cloned().unwrap_or_default();
            let tls = build_tls(&qm, "");
            let transport = build_transport(&qm);
            #[allow(clippy::option_if_let_else, reason = "None 分支含多步链式调用，map_or_else 可读性更差")]
            let uuid2 = match info.userinfo.as_deref().filter(|s| !s.is_empty()) {
                Some(u) => u.to_string(),
                None => node
                    .cred
                    .split("://")
                    .nth(1)
                    .and_then(|s| s.split('@').next())
                    .unwrap_or("")
                    .to_string(),
            };
            Ok(json!({
                "type": "vless",
                "tag": "proxy",
                "server": node.addr,
                "server_port": node.port,
                "uuid": uuid2,
                "flow": flow,
                "tls": tls,
                "transport": transport
            }))
        }
        crate::model::NodeType::Vmess => {
            let qm = parse_query_map(info.query.as_deref());
            let tls = build_tls(&qm, "");
            let transport = build_transport(&qm);
            #[allow(clippy::option_if_let_else, reason = "同上")]
            let uuid2 = match info.userinfo.as_deref().filter(|s| !s.is_empty()) {
                Some(u) => u.to_string(),
                None => node
                    .cred
                    .split("://")
                    .nth(1)
                    .and_then(|s| s.split('@').next())
                    .and_then(|s| s.split('?').next())
                    .unwrap_or("")
                    .to_string(),
            };
            let security = qm.get("scy").cloned().unwrap_or_else(|| "auto".to_string());
            Ok(json!({
                "type": "vmess",
                "tag": "proxy",
                "server": node.addr,
                "server_port": node.port,
                "uuid": uuid2,
                "security": security,
                "tls": tls,
                "transport": transport
            }))
        }
        crate::model::NodeType::Trojan => {
            let qm = parse_query_map(info.query.as_deref());
            let tls = build_tls(&qm, &node.addr);
            let transport = build_transport(&qm);
            Ok(json!({
                "type": "trojan",
                "tag": "proxy",
                "server": node.addr,
                "server_port": node.port,
                "password": info.userinfo.unwrap_or_default(),
                "tls": if tls.as_object().is_none_or(Map::is_empty) { json!({"enabled": true, "server_name": node.addr}) } else { tls },
                "transport": transport
            }))
        }
        crate::model::NodeType::Shadowsocks => {
            let (method, password) = match info.userinfo.as_deref() {
                Some(s) if s.contains(':') => {
                    let mut it = s.splitn(2, ':');
                    (
                        it.next().unwrap_or("aes-256-gcm").to_string(),
                        it.next().unwrap_or("").to_string(),
                    )
                }
                Some(s) if !s.is_empty() => ("aes-256-gcm".to_string(), s.to_string()),
                _ => ("aes-256-gcm".to_string(), String::new()),
            };
            if method.is_empty() || password.is_empty() {
                return Err(anyhow!(
                    "Shadowsocks 节点缺少 method/password: {}",
                    node.cred
                ));
            }
            Ok(json!({
                "type": "shadowsocks",
                "tag": "proxy",
                "server": node.addr,
                "server_port": node.port,
                "method": method,
                "password": password
            }))
        }
        crate::model::NodeType::Hysteria2 => {
            let qm = parse_query_map(info.query.as_deref());
            let password = info.userinfo.unwrap_or_default();
            if password.is_empty() {
                return Err(anyhow!("Hysteria2 节点缺少密码: {}", node.cred));
            }
            let mut tls = build_tls(&qm, &node.addr);
            if tls.as_object().is_none_or(Map::is_empty) {
                tls = json!({"enabled": true, "server_name": node.addr});
            }
            Ok(json!({
                "type": "hysteria2",
                "tag": "proxy",
                "server": node.addr,
                "server_port": node.port,
                "password": password,
                "tls": tls
            }))
        }
        crate::model::NodeType::Socks => {
            let (username, password) = split_userinfo(info.userinfo.as_deref());
            let mut out = json!({
                "type": "socks",
                "tag": "proxy",
                "server": node.addr,
                "server_port": node.port
            });
            if let Some(obj) = out.as_object_mut() {
                if !username.is_empty() {
                    obj.insert("username".into(), json!(username));
                }
                if !password.is_empty() {
                    obj.insert("password".into(), json!(password));
                }
            }
            Ok(out)
        }
        crate::model::NodeType::Tuic => {
            let (uuid, password) = split_userinfo(info.userinfo.as_deref());
            if uuid.is_empty() || password.is_empty() {
                return Err(anyhow!("TUIC 节点缺少 uuid/password: {}", node.cred));
            }
            let qm = parse_query_map(info.query.as_deref());
            let tls = build_tls(&qm, &node.addr);
            let mut out = json!({
                "type": "tuic",
                "tag": "proxy",
                "server": node.addr,
                "server_port": node.port,
                "uuid": uuid,
                "password": password,
                "tls": tls
            });
            if let Some(obj) = out.as_object_mut() {
                for key in ["congestion_control", "udp_relay_mode", "zero_rtt_handshake"] {
                    if let Some(value) = qm.get(key) {
                        obj.insert(key.into(), json!(value));
                    }
                }
            }
            Ok(out)
        }
        crate::model::NodeType::AnyTls => {
            let password = info.userinfo.clone().unwrap_or_default();
            if password.is_empty() {
                return Err(anyhow!("AnyTLS 节点缺少密码: {}", node.cred));
            }
            let qm = parse_query_map(info.query.as_deref());
            Ok(json!({
                "type": "anytls",
                "tag": "proxy",
                "server": node.addr,
                "server_port": node.port,
                "password": password,
                "tls": build_tls(&qm, &node.addr)
            }))
        }
        crate::model::NodeType::Naive => {
            let (username, password) = split_userinfo(info.userinfo.as_deref());
            if username.is_empty() || password.is_empty() {
                return Err(anyhow!("Naive 节点缺少用户名/密码: {}", node.cred));
            }
            Ok(json!({
                "type": "naive",
                "tag": "proxy",
                "server": node.addr,
                "server_port": node.port,
                "username": username,
                "password": password,
                "tls": {"enabled": true, "server_name": node.addr}
            }))
        }
        crate::model::NodeType::Http => {
            let (username, password) = split_userinfo(info.userinfo.as_deref());
            let tls = if node.cred.to_ascii_lowercase().starts_with("https://") {
                json!({"enabled": true, "server_name": node.addr})
            } else {
                json!({})
            };
            let mut out = json!({
                "type": "http",
                "tag": "proxy",
                "server": node.addr,
                "server_port": node.port,
                "tls": tls
            });
            if let Some(obj) = out.as_object_mut() {
                if !username.is_empty() {
                    obj.insert("username".into(), json!(username));
                }
                if !password.is_empty() {
                    obj.insert("password".into(), json!(password));
                }
            }
            Ok(out)
        }
        crate::model::NodeType::Wireguard | crate::model::NodeType::Unknown(_) => Err(anyhow!(
            "不支持生成 sing-box outbound 的协议: {}",
            node.r#type.as_str()
        )),
    }
}

fn split_userinfo(userinfo: Option<&str>) -> (String, String) {
    let Some(value) = userinfo else {
        return (String::new(), String::new());
    };
    let mut parts = value.splitn(2, ':');
    (
        parts.next().unwrap_or_default().to_string(),
        parts.next().unwrap_or_default().to_string(),
    )
}

// 生成多入站多出站的 sing-box config（单进程服务 N 端口）
/// `listen_addr` 为入站监听地址（127.0.0.1 仅本机 / 0.0.0.0 允许内网访问）
/// include 非空时启用 include 模式：命中域名后缀/CIDR 的流量走对应节点出口，其余直连
pub fn generate_singbox_config(
    nodes: &[(&Node, u16)],
    listen_addr: &str,
    include: &[String],
) -> Result<Value> {
    let (domain_suffix, ip_cidr) = split_include(include)?;
    let mut inbounds = Vec::new();
    let mut outbounds: Vec<Value> = Vec::new();
    let mut route_rules = Vec::new();

    // include 模式下客户端可能拿解析后的 IP 直连，需 sniff 出 TLS SNI / HTTP Host 才能按域名匹配
    if !include.is_empty() {
        route_rules.push(json!({"action": "sniff"}));
    }

    for (idx, (node, port)) in nodes.iter().enumerate() {
        let in_tag = format!("in-{port}");
        inbounds.push(json!({
            "type": "mixed",
            "tag": in_tag,
            "listen": listen_addr,
            "listen_port": port
        }));
        let mut ob = node_to_singbox_outbound(node)?;
        let ob_tag = format!("proxy-{idx}");
        if let Some(o) = ob.as_object_mut() {
            o.insert("tag".to_string(), Value::String(ob_tag.clone()));
        }
        outbounds.push(ob);
        if include.is_empty() {
            route_rules.push(json!({
                "inbound": in_tag,
                "outbound": ob_tag
            }));
        } else {
            // 命中才走节点，未命中落 final: direct
            route_rules.push(json!({
                "inbound": in_tag,
                "domain_suffix": domain_suffix,
                "ip_cidr": ip_cidr,
                "outbound": ob_tag
            }));
        }
    }
    outbounds.push(json!({"type":"direct","tag":"direct"}));
    outbounds.push(json!({"type":"block","tag":"block"}));

    let cfg = json!({
        "log": {"level":"warn"},
        "inbounds": inbounds,
        "outbounds": outbounds,
        "route": {"rules": route_rules, "final": "direct"}
    });
    Ok(cfg)
}

/// CIDR 校验：addr 解析为 IP 且前缀长度不超限（v4≤32 / v6≤128）
fn is_valid_cidr(item: &str) -> bool {
    let Some((ip, prefix)) = item.split_once('/') else {
        return false;
    };
    let max = match ip.parse::<IpAddr>() {
        Ok(IpAddr::V4(_)) => 32,
        Ok(IpAddr::V6(_)) => 128,
        Err(_) => return false,
    };
    prefix.parse::<u8>().is_ok_and(|p| p <= max)
}

/// include 条目归一化：`*.x` 去掉 `*.` 与裸域同为后缀匹配（去重）；CIDR 单独归类
/// 返回 (`domain_suffix`, `ip_cidr`)，非法 CIDR 报错（加载期即暴露配置错误）
pub fn split_include(entries: &[String]) -> Result<(Vec<String>, Vec<String>)> {
    let mut domain_suffix: Vec<String> = Vec::new();
    let mut ip_cidr: Vec<String> = Vec::new();
    for e in entries {
        let item = e.trim();
        if item.is_empty() {
            continue;
        }
        if item.contains('/') {
            if !is_valid_cidr(item) {
                return Err(anyhow!("include 条目非法 CIDR: \"{item}\"（示例 172.64.128.0/20）"));
            }
            if !ip_cidr.iter().any(|x| x == item) {
                ip_cidr.push(item.to_string());
            }
        } else {
            let domain = item.strip_prefix("*.").unwrap_or(item).to_ascii_lowercase();
            if !domain_suffix.iter().any(|x| x == &domain) {
                domain_suffix.push(domain);
            }
        }
    }
    Ok((domain_suffix, ip_cidr))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fmt::parse_uri;

    #[test]
    fn test_include_mode_routes() {
        let n = parse_uri("vless://uuid@1.1.1.1:443?security=tls#t", "1").unwrap();
        let include: Vec<String> = vec![
            "github.com".into(),
            "*.github.com".into(),
            "172.64.128.0/20".into(),
        ];
        let cfg = generate_singbox_config(&[(&n, 18282)], "0.0.0.0", &include).unwrap();
        let rules = cfg["route"]["rules"].as_array().unwrap();
        // 首条为 sniff，随后每个入站一条命中规则（inbound+domain_suffix+ip_cidr），无兜底入站规则
        assert_eq!(rules[0]["action"], "sniff");
        let route = &rules[1];
        assert_eq!(route["inbound"], "in-18282");
        assert_eq!(route["outbound"], "proxy-0");
        // `*.github.com` 与 `github.com` 归一化去重，只剩一条后缀
        let suffixes: Vec<&str> =
            route["domain_suffix"].as_array().unwrap().iter().map(|v| v.as_str().unwrap()).collect();
        assert_eq!(suffixes, vec!["github.com"]);
        assert_eq!(
            route["ip_cidr"].as_array().unwrap().iter().map(|v| v.as_str().unwrap()).collect::<Vec<_>>(),
            vec!["172.64.128.0/20"]
        );
        assert_eq!(cfg["route"]["final"], "direct");
        assert_eq!(rules.len(), 2);
    }

    #[test]
    fn test_no_include_keeps_legacy_route() {
        let n = parse_uri("vless://uuid@1.1.1.1:443?security=tls#t", "1").unwrap();
        let cfg = generate_singbox_config(&[(&n, 18282)], "0.0.0.0", &[]).unwrap();
        let rules = cfg["route"]["rules"].as_array().unwrap();
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0]["inbound"], "in-18282");
        assert_eq!(rules[0]["outbound"], "proxy-0");
        assert!(rules[0]["domain_suffix"].is_null());
    }

    #[test]
    fn test_split_include_normalization() {
        let (d, c) = split_include(&[
            "github.com".into(),
            "  *.GitHub.COM  ".into(),
            String::new(),
            "172.64.128.0/20".into(),
            "2606:4700:cf1::/48".into(),
        ])
        .unwrap();
        // 通配去前缀、大小写归一、去重
        assert_eq!(d, vec!["github.com"]);
        assert_eq!(c, vec!["172.64.128.0/20", "2606:4700:cf1::/48"]);
    }

    #[test]
    fn test_inbound_is_mixed_for_http_and_socks() {
        let n = parse_uri("vless://uuid@1.1.1.1:443?security=tls#t", "1").unwrap();
        let cfg = generate_singbox_config(&[(&n, 18282)], "0.0.0.0", &[]).unwrap();
        let inbound = &cfg["inbounds"][0];
        assert_eq!(inbound["type"].as_str(), Some("mixed"));
        assert_eq!(inbound["listen_port"].as_u64(), Some(18282));
        assert_eq!(inbound["listen"].as_str(), Some("0.0.0.0"));
    }

    #[test]
    fn test_ss_method_split() {
        let n = parse_uri(
            "ss://Y2hhY2hhMjAtaWV0Zi1wb2x5MTMwNTpwYXNz@1.1.1.1:8388#t",
            "1",
        )
        .unwrap();
        let ob = node_to_singbox_outbound(&n).unwrap();
        assert_eq!(
            ob.get("method").and_then(|v| v.as_str()),
            Some("chacha20-ietf-poly1305")
        );
        assert_eq!(ob.get("password").and_then(|v| v.as_str()), Some("pass"));
    }

    #[test]
    fn test_vless_ws_transport() {
        let n = parse_uri("vless://uuid@1.1.1.1:443?encryption=none&security=tls&type=ws&host=example.com&path=%2Fws#t", "1").unwrap();
        let ob = node_to_singbox_outbound(&n).unwrap();
        assert_eq!(ob.get("type").and_then(|v| v.as_str()), Some("vless"));
        let tls = ob.get("tls").unwrap();
        assert_eq!(tls.get("enabled").and_then(serde_json::Value::as_bool), Some(true));
        let tr = ob.get("transport").unwrap();
        assert_eq!(tr.get("type").and_then(|v| v.as_str()), Some("ws"));
        assert_eq!(tr.get("path").and_then(|v| v.as_str()), Some("/ws"));
    }

    #[test]
    fn test_vless_tcp_no_tls() {
        let n = parse_uri(
            "vless://uuid@1.1.1.1:8080?security=none&encryption=none&type=tcp#t",
            "1",
        )
        .unwrap();
        let ob = node_to_singbox_outbound(&n).unwrap();
        assert!(ob.get("tls").unwrap().as_object().unwrap().is_empty());
    }

    #[test]
    fn test_ws_host_does_not_enable_tls_without_security() {
        let n = parse_uri(
            "vless://uuid@1.1.1.1:80?encryption=none&type=ws&host=cdn.example&path=%2Fws#t",
            "1",
        )
        .unwrap();
        let ob = node_to_singbox_outbound(&n).unwrap();
        assert!(ob["tls"]["enabled"].is_null());
    }

    #[test]
    fn test_reality_without_fp_gets_default_utls() {
        // reality 缺 fp 时必须补默认指纹，否则 sing-box 启动即 FATAL（v2rayN 自带默认值能连）
        let n = parse_uri(
            "vless://u@1.1.1.1:443?encryption=none&security=reality&pbk=PUBKEY&sid=abc#r",
            "1",
        )
        .unwrap();
        let ob = node_to_singbox_outbound(&n).unwrap();
        assert_eq!(ob["tls"]["utls"]["fingerprint"].as_str(), Some("chrome"));
        assert_eq!(ob["tls"]["reality"]["public_key"].as_str(), Some("PUBKEY"));
    }

    #[test]
    fn test_reality_keeps_explicit_fp() {
        let n = parse_uri(
            "vless://u@1.1.1.1:443?encryption=none&security=reality&pbk=PUBKEY&fp=firefox#r",
            "1",
        )
        .unwrap();
        let ob = node_to_singbox_outbound(&n).unwrap();
        assert_eq!(ob["tls"]["utls"]["fingerprint"].as_str(), Some("firefox"));
    }

    #[test]
    fn test_plain_tls_without_fp_has_no_utls() {
        // 非 reality 不得凭空加 utls：无 fp 时沿用 sing-box 默认 TLS 栈
        let n = parse_uri("vless://u@1.1.1.1:443?encryption=none&security=tls#t", "1").unwrap();
        let ob = node_to_singbox_outbound(&n).unwrap();
        assert!(ob["tls"]["utls"].is_null());
    }

    #[test]
    fn test_hysteria2_keeps_password_and_tls_parameters() {
        let n = parse_uri(
            "hysteria2://secret@1.1.1.1:443/?sni=example.com&insecure=1#hy",
            "1",
        )
        .unwrap();
        let ob = node_to_singbox_outbound(&n).unwrap();
        assert_eq!(ob["password"].as_str(), Some("secret"));
        assert_eq!(ob["tls"]["enabled"].as_bool(), Some(true));
        assert_eq!(ob["tls"]["server_name"].as_str(), Some("example.com"));
    }

    #[test]
    fn test_http_node_is_not_silently_direct() {
        let n = parse_uri("http://user:pass@1.1.1.1:8080#http", "1").unwrap();
        let ob = node_to_singbox_outbound(&n).unwrap();
        assert_ne!(ob["type"].as_str(), Some("direct"));
    }

    #[test]
    fn test_https_proxy_keeps_tls() {
        let n = parse_uri("https://user:pass@1.1.1.1:8443#https", "1").unwrap();
        let ob = node_to_singbox_outbound(&n).unwrap();
        assert_eq!(ob["tls"]["enabled"].as_bool(), Some(true));
    }

    #[test]
    fn test_unsupported_protocol_returns_error_instead_of_direct() {
        let n = parse_uri("wireguard://key@1.1.1.1:51820#wg", "1").unwrap();
        assert!(node_to_singbox_outbound(&n).is_err());
    }
}
