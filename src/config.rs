use std::path::PathBuf;

use crate::model::Settings;

/// config.toml 路径（~/.config/proxytool/config.toml）
pub fn config_path() -> PathBuf {
    let dir = dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("proxytool");
    std::fs::create_dir_all(&dir).ok();
    dir.join("config.toml")
}

/// 默认模板（首次运行自动写出）
fn default_toml() -> String {
    r#"# proxytool 配置

# 出口 IP 查询网址（get ip）
ip_api_url = "https://api.ip.sb/geoip"

# 连通性基准网址：switch 切换后自动实测它，不通自动顺延下一个节点
speed_ping_url = "https://chatgpt.com"

# 本地代理入站监听地址：127.0.0.1 仅本机可用，0.0.0.0 允许内网其他机器访问
# 注意：0.0.0.0 无认证，局域网内等同于开放代理，仅限可信网络使用
listen_addr = "0.0.0.0"

# 测速参数
test_concurrency = 32
timeout_secs = 5
page_size = 1000

# 常驻看护：多久探测一次经代理访问基准网址
watch_interval_secs = 30
# 单次看护探测超时
watch_timeout_secs = 10
# 连续失败几次才切换
watch_fail_threshold = 2
# 切换冷却（秒），防抖动
watch_cooldown_secs = 60
# 流式替换阈值：新节点速度超出现役该倍率才替换
replace_speed_ratio = 1.10
"#
    .to_string()
}

/// 加载 config.toml；不存在则生成默认模板后加载；解析失败报错并指出文件位置
pub fn load_or_create() -> anyhow::Result<Settings> {
    let path = config_path();
    if !path.exists() {
        std::fs::write(&path, default_toml())?;
        println!("已生成默认配置 {}", path.display());
    }
    let s = std::fs::read_to_string(&path)?;
    toml::from_str(&s).map_err(|e| {
        anyhow::anyhow!("配置解析失败 {}: {e}（可删除该文件后重跑以重新生成默认配置）", path.display())
    })
}

/// 畸形节点判定：内网/保留地址、localhost、端口 0（入口过滤 + prune 共用）
pub fn is_bogus_endpoint(addr: &str, port: u16) -> bool {
    if port == 0 {
        return true;
    }
    if addr.eq_ignore_ascii_case("localhost") {
        return true;
    }
    match addr.parse::<std::net::IpAddr>() {
        Ok(ip) => match ip {
            std::net::IpAddr::V4(v4) => {
                v4.is_loopback() || v4.is_private() || v4.is_link_local() || v4.is_unspecified()
            }
            std::net::IpAddr::V6(v6) => v6.is_loopback() || v6.is_unspecified(),
        },
        // 域名无法静态判定，放行
        Err(_) => false,
    }
}

#[cfg(test)]
mod config_tests {
    use super::*;

    #[test]
    fn test_default_toml_parses() {
        let s: Settings = toml::from_str(&default_toml()).unwrap();
        assert_eq!(s.speed_ping_url, "https://chatgpt.com");
        assert_eq!(s.ip_api_url, "https://api.ip.sb/geoip");
        assert_eq!(s.listen_addr, "0.0.0.0");
        assert_eq!(s.test_concurrency, 32);
        assert_eq!(s.timeout_secs, 5);
        assert_eq!(s.page_size, 1000);
        assert_eq!(s.watch_interval_secs, 30);
        assert_eq!(s.watch_timeout_secs, 10);
        assert_eq!(s.watch_fail_threshold, 2);
        assert_eq!(s.watch_cooldown_secs, 60);
        assert!((s.replace_speed_ratio - 1.10).abs() < 1e-9);
    }

    #[test]
    fn test_old_toml_without_watch_fields_still_parses() {
        // 存量配置无看护字段，用默认值兼容
        let s: Settings = toml::from_str(
            r#"ip_api_url = "https://api.ip.sb/geoip"
speed_ping_url = "https://chatgpt.com"
test_concurrency = 32
timeout_secs = 5
page_size = 1000
"#,
        )
        .unwrap();
        assert_eq!(s.watch_interval_secs, 30);
        assert_eq!(s.listen_addr, "0.0.0.0");
        assert!((s.replace_speed_ratio - 1.10).abs() < 1e-9);
    }

    #[test]
    fn test_bogus_endpoints() {
        assert!(is_bogus_endpoint("127.0.0.1", 443));
        assert!(is_bogus_endpoint("192.168.1.1", 443));
        assert!(is_bogus_endpoint("10.0.0.1", 443));
        assert!(is_bogus_endpoint("172.16.0.1", 443));
        assert!(is_bogus_endpoint("169.254.1.1", 443));
        assert!(is_bogus_endpoint("0.0.0.0", 443));
        assert!(is_bogus_endpoint("::1", 443));
        assert!(is_bogus_endpoint("localhost", 443));
        assert!(is_bogus_endpoint("8.8.8.8", 0));
        assert!(!is_bogus_endpoint("8.8.8.8", 443));
        assert!(!is_bogus_endpoint("example.com", 443));
        assert!(!is_bogus_endpoint("2001:db8::1", 443)); // 文档段无法静态判定，放行
    }
}
