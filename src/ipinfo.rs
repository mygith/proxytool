use std::time::Duration;
use crate::model::Node;
use serde::Deserialize;
use serde_json::from_str;
use std::sync::Arc;

#[derive(Debug, Deserialize)]
struct IpApi {
    ip: Option<String>,
    #[serde(rename = "clientIp")]
    client_ip: Option<String>,
    #[serde(rename = "ip_addr")]
    ip_addr: Option<String>,
    query: Option<String>,
    #[serde(rename = "country_code")]
    country_code: Option<String>,
    country: Option<String>,
    #[serde(rename = "countryCode")]
    country_code2: Option<String>,
    location: Option<Location>,
}
#[derive(Debug, Deserialize)]
struct Location {
    country_code: Option<String>,
}

pub async fn fetch_ip_via_proxy(
    proxy_url: &str,
    api_url: &str,
    timeout_secs: u64,
) -> Option<(String, String)> {
    let proxy = reqwest::Proxy::all(proxy_url).ok()?;
    let client = reqwest::Client::builder()
        .proxy(proxy)
        .timeout(Duration::from_secs(timeout_secs))
        .build()
        .ok()?;
    let txt = client.get(api_url).send().await.ok()?.text().await.ok()?;
    parse_ip_api(&txt)
}

pub fn parse_ip_api(txt: &str) -> Option<(String, String)> {
    let v: IpApi = from_str(txt).ok()?;
    let ip = v.ip.or(v.client_ip).or(v.ip_addr).or(v.query)?;
    let cc = v
        .country_code
        .or(v.country_code2)
        .or(v.country)
        .or(v.location.and_then(|l| l.country_code))
        .unwrap_or_else(|| "unknown".to_string());
    Some((ip, cc))
}

/// 直连获取本机公网 IP（对照组，用于与代理出口对比）
pub async fn fetch_my_ip(api_url: &str, timeout_secs: u64) -> anyhow::Result<(String, String)> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(timeout_secs))
        .user_agent("proxytool/0.1")
        .build()?;
    let txt = client.get(api_url).send().await?.text().await?;
    parse_ip_api(&txt).ok_or_else(|| anyhow::anyhow!("解析本机 IP 失败: {txt:.200}"))
}

pub async fn fetch_ipinfo_for_nodes(nodes: &mut [Node], concurrency: usize, api_url: &str) {
    // 直连查询（对照/补查用）；真实出口 IP 应走代理（见 probe）
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .unwrap();
    let api = api_url.to_string();
    let sem = Arc::new(tokio::sync::Semaphore::new(concurrency.max(1)));
    let mut handles = Vec::new();
    for (idx, n) in nodes.iter().enumerate() {
        if !n.alive {
            continue;
        }
        let api = api.clone();
        let sem = sem.clone();
        let client = client.clone();
        handles.push(tokio::spawn(async move {
            let _p = sem.acquire().await.unwrap();
            let txt = client.get(&api).send().await.ok()?.text().await.ok()?;
            let v: IpApi = from_str::<IpApi>(&txt).ok()?;
            let ip = v.ip.or(v.client_ip).or(v.ip_addr).or(v.query)?;
            let cc = v
                .country_code
                .or(v.country_code2)
                .or(v.country)
                .or(v.location.and_then(|l| l.country_code))
                .unwrap_or_else(|| "unknown".to_string());
            Some((idx, ip, cc))
        }));
    }
    for h in handles {
        if let Ok(Some((idx, ip, cc))) = h.await
            && let Some(n) = nodes.get_mut(idx)
        {
            n.exit_ip = Some(ip);
            n.cc = Some(cc);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_parse_ip_sb() {
        let txt = r#"{"ip":"162.221.196.52","country_code":"DE","country":"Germany"}"#;
        let (ip, cc) = parse_ip_api(txt).unwrap();
        assert_eq!(ip, "162.221.196.52");
        assert_eq!(cc, "DE");
    }
    #[test]
    fn test_parse_ip_sb_query_fallback() {
        let txt = r#"{"query":"1.1.1.1","countryCode":"US"}"#;
        let (ip, cc) = parse_ip_api(txt).unwrap();
        assert_eq!(ip, "1.1.1.1");
        assert_eq!(cc, "US");
    }
}
