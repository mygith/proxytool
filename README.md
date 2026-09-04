# proxytool

高性能多协议订阅 / 测速 / 多出口代理 CLI（Rust，配置适配 sing-box 全协议）。

**架构（B：C/S 单写者）**：`proxytool serve` **默认后台**常驻单写者 server（内存态为真源、单线程串行落盘、托管全部 sing-box 与看护），CLI 是瘦客户端——写命令经 Unix socket RPC 提交（**server 未运行时自动后台拉起，无需先 serve**），读命令（list/status/export/myip）直读 SQLite 永不受 server 存活影响。空载 server 实测 CPU≈0、RSS≈9MB，保持常驻不自动退出。

## 功能

- 订阅抓取与解析：支持明文按行、整体 base64、单行 base64；支持 vless / vmess（旧 base64 JSON 与新 URI）/ trojan / ss（多种 SIP002 变体）/ hysteria2 / tuic / socks 等
- 解析后自动去重：`uri` 去重 + `ip:port` 去重（保留首次，大小写归一；空地址/0 端口不合并），跨订阅再去重一次
- 三种测速：`tcping`（全量快筛）、`realping`（逐节点经临时 sing-box 代理访问目标）、`hybrid`（tcping 全量取 top 再真实复测）
- 真实探测 `probe`：逐个节点起临时 sing-box，经本地 socks 抓 `https://www.google.com/`（成功=2xx/3xx），延迟=耗时，速度=页面大小/耗时，失败回退 `generate_204` 保活；小批量逐批，全量测完取速度最快（速度优先、延迟其次）
- 一键全流程 `auto`：更新订阅 -> 测速 -> 剪枝 -> 流式探测（**有可用立刻上线，速度更快超 10% 立刻替换**）-> 常驻看护；job 始终在 server 内后台执行，Ctrl+C 只退出 CLI 回显不影响执行
- 常驻看护（server 内任务）：周期经代理实测目标网址，连续 `watch_fail_threshold` 次失败或 sing-box 进程死亡立即自动更换；冷却防抖
- 本机公网 IP 对照（`myip`）与代理出口 IP 对比
- 单/多出口代理：生成 sing-box 配置并启动，支持 `daemon`、`distinct-cc`、`random` 策略、`--filter` 精选节点
- 节点健康：订阅解析时过滤内网/保留地址与端口 0 的畸形节点；切换/看护实测基准网址，假活节点（出口也不通）当场删除；`prune --invalid` 清理存量畸形节点
- 存储：SQLite（`state.db`，WAL），节点表端点部分唯一索引；运行态唯一真源为 `running` 表（**server 运行期间唯一写者是 server，请勿直改 state.db**）

## 配置

`~/.config/proxytool/config.toml`（首次运行自动生成）：

```toml
# 出口 IP 查询网址
ip_api_url = "https://api.ip.sb/geoip"
# 连通性基准网址（switch 切换后自动实测）
speed_ping_url = "https://chatgpt.com"
# 测速参数
test_concurrency = 32
timeout_secs = 5
page_size = 1000

# 常驻看护：多久探测一次经代理访问目标网址
watch_interval_secs = 30
# 单次看护探测超时
watch_timeout_secs = 10
# 连续失败几次才切换
watch_fail_threshold = 2
# 切换冷却（秒），防抖动
watch_cooldown_secs = 60
# 流式替换阈值：新节点速度超出现役该倍率才替换
replace_speed_ratio = 1.10
```

## 依赖

- Rust 工具链（`cargo build`）
- `sing-box` 二进制（`probe` / `run` / `auto` / `test --mode realping|hybrid` 必需，`test --mode tcping` 不需要）：
  `https://github.com/SagerNet/sing-box/releases`，解压后放入 `PATH`（如 `~/.local/bin`），`sing-box version` 验证

## 构建

```bash
cargo build            # 调试版
cargo build --release  # 发布版（LTO）
cargo test             # 单测
```

## 配置订阅清单

默认 `~/.config/proxytool/subs.json`（可用全局 `--subs <path>` 覆盖），格式 `[{name?,url}]`，`name` 缺省按序号 `1,2...` 补齐：

```json
[{"name":"barry","url":"https://example.com/All_Configs_Sub.txt"}]
```

注意：读清单不自动更新节点，必须显式 `sub update`（避免重复更新）。

## 数据文件

`~/.local/share/proxytool/`（`dirs::data_local_dir`）：

- `state.db`：主库（nodes/subs/running/meta）
- `server.sock`：server RPC 通道（`serve --stop` 或退出时清理；残留无响应会自动重建）
- `singbox-<ts>.json/.log`：生成的代理配置与日志（保留最新 20 组，`stop` 删配置留日志）
- `serve-<ts>.log`：server 自身日志（`serve --daemon` / 自动拉起时）
- `job-<kind>-<id>.log`：每个后台 job 的执行日志（auto/probe/test/run/stop/switch 等，保留最新 20 个）

## 常用命令（端到端）

```bash
# 0. server（默认后台：写命令会自动拉起，也可显式启动）
proxytool serve              # 后台常驻：父进程分叉，打印 pid + 日志后立即返回（Ctrl+C 不影响）
proxytool serve --foreground # 前台调试（Ctrl+C 优雅退出：停看护/job/代理）
proxytool serve --stop       # 停止 server（含托管的所有代理与看护）

# 1. 订阅
proxytool sub list
proxytool sub update                 # 抓取全部；--name <订阅名> 只更新单个

# 2. 本机公网 IP（对照组，直连执行）
proxytool myip

# 3. 快筛（tcping 全量）
proxytool test --mode tcping --concurrency 200 --timeout 3

# 4. 真实探测：小批量逐批，全量测完取最快（默认首页，可改 --probe-url）
proxytool probe --batch-size 15 --max-batches 60 --concurrency 5 --timeout 12
proxytool probe --filter "香港|HK" --batch-size 10 --max-batches 20  # 正则过滤 sub/协议/地址/凭证

# 5. 查看（list/status/export 直读 DB，server 死活不影响）
proxytool list --alive-only          # --sort delay|speed|cc --json
proxytool status                     # server 状态 / 端口 / 看护标记

# 6. 启动代理（需 sing-box；单端口 daemon 失败自动顺延 --retries，默认 3）
proxytool run --port 10808 --daemon                      # 延迟最低的存活节点
proxytool run --port 18282 --daemon --filter "1.2.3.4"   # 精选节点
proxytool run --ports 10808,10809 --daemon --distinct-cc # 多出口不同国家优先

# 7. 验证出口（与 myip 对比 IP 是否变化；入站为 mixed，同端口兼容 socks5h/http）
curl -x socks5h://127.0.0.1:10808 -m 15 -s https://api.ip.sb/geoip
curl -x http://127.0.0.1:10808 -m 15 -s https://api.ip.sb/geoip
curl -x socks5h://127.0.0.1:10808 -m 20 -s -o /dev/null -w "%{http_code} %{time_total}s\n" https://www.google.com/

# 8. 停止（联动停看护）
proxytool stop --port 10808
proxytool stop --all

# 9. 维护
proxytool prune --dedup-endpoint          # 端点去重整理（保留未测节点）
proxytool prune --invalid                 # 清理内网地址/端口 0 的畸形节点
proxytool prune --delay-threshold 0 --keep-top 500
proxytool export --alive-only --output alive.txt   # --format uri|json
proxytool switch --port 10808             # 热切换：改映射并重启 sing-box，自动实测基准网址；不通自动顺延（假活节点当场删除）
proxytool switch --all                    # 整体轮换全部端口（不做逐节点验证）
proxytool switch --port 10808 --no-restart # 仅改映射不重启不验证（需手动 run 生效）
proxytool switch --port 10808 --which prev # next/prev/random/index:N
proxytool ipinfo --concurrency 16         # 直连补查（仅参考；真实出口 IP 以 probe 为准）

# 10. 一键全流程（更新订阅 -> 测速 -> 去除失效 -> 流式探测 -> 上线 -> 常驻看护）
proxytool auto --port 18282               # 默认提交即返回（job 在 server 内后台跑）；--follow 实时回显
proxytool auto --follow --port 18282
proxytool auto --name barry --filter "香港|HK" --port 18282   # 只更新单个订阅并过滤节点
proxytool auto --skip-update --skip-test --port 18282        # 库里已有节点时跳过前面步骤
# 探测流式上线：有可用节点立刻起代理；后续测出快 10% 的节点立刻替换（replace_speed_ratio 可调）
# probe 全落空时回退 tcping 候选兜底顺延；常驻看护失活自动更换（watch_* 参数见 config.toml）
# 保活型节点（仅通保活、打不开首页）会被 probe/prune 自动删除；run 单端口失败自动试下一个
# 注意：server 运行期间 state.db 的唯一写者是 server，请勿外部直改库
```

## 架构（B：C/S 单写者）

```
CLI（瘦客户端）                          server（serve 常驻，唯一写者）
  list/status/export/myip ──直读 WAL──▶  state.db
  sub update/test/probe/auto/run/        ├─ db-writer 线程（rusqlite 串行）
  stop/switch/ipinfo ──RPC(server.sock)─►├─ supervisor：launch/failover/replace（sing-box 子进程，process_group(0)）
                                          ├─ watch 任务：周期实测，失活即换（按 meta 恢复）
                                          └─ job 注册表：kind/log/结果（CLI tail job-*.log）
```

- 写命令（sub update/test/prune/probe/auto/run/stop/switch/ipinfo）→ job 提交 → CLI tail 对应 `job-*.log` 直到完成；CLI 被 Ctrl+C 掐掉只断回显，job 继续
- 读命令（list/status/export/myip）直读 SQLite（WAL 并发读安全），不依赖 server
- server 崩溃/被杀：sing-box 因 `process_group(0)` 存活，下次启动 re-adopt（`running` 表按 pid 校验），看护按 `meta watch:<port>` 恢复

## 节点字段（精简存储）

`[sub（订阅名）/ proto（协议）/ addr / port / cred（完整凭证 URI）] + [exit_ip（经该节点出口 IP）/ cc / delay_ms / speed_kbps / alive / last_test_at]`，不存别名。`filter` 正则匹配 `sub/协议/地址/凭证`。

## 说明

- `test --mode tcping` 只代表端口可连，不代表代理可用；以 `probe`（真实走代理抓 google）为准
- 免费订阅节点存活短、轮换快，`probe` 找不到可用时扩大 `--max-batches` 或先 `sub update` 刷新
- `status` 显示 `看护=server` 表示该端口有常驻看护；`switch` 的人为切换会与看护并存，看护只管"失活即换"，不干预人工选择
