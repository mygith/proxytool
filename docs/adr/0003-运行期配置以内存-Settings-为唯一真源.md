# ADR-0003：运行期配置以内存 Settings 为唯一真源

- 状态：已接受
- 日期：2026-09-12

## 背景

server 运行期存在两个配置真源：

- **磁盘**：`config::load_or_create()` 在 `cmd.rs`、`probe.rs`、`flow.rs`、
  `watch.rs`、`server.rs` 共 9 处被反复调用，每次重新读并解析 config.toml；
- **内存**：`AppState.settings`，仅在启动时由 `server.rs:94` /
  `store/mod.rs:200` 填充一次，此后**从不刷新**。

由此产生一个静默 bug：用户改了 `probe_url` 却不重启时，`cmd.rs:214` 用**新的**
probe_url 去做验证，而 `proxy.rs:67` 的 `effective_include`（`proxy.rs:13-26`，
派生追加 probe_url 的 host）仍从冻结的内存快照读**旧的** probe_url。新目标的
host 不在 include 白名单里 → 验证请求落 `final` 直连 → 恒为通过 → 看护与
failover 完全失效。`proxy.rs:11-12` 的注释写的正是这个场景。

## 决策

**运行期一律从 `AppState.settings` 读配置。** 磁盘只在两个加载边界被读：
`server.rs:94`（server 启动）、`store/mod.rs:200`（CLI 直读数据库路径）。

配套变更：

- 新增 `Ctx::settings()`（`src/ctx.rs:76`）只读入口，避免为取一个 Settings
  而全量 clone `AppState`（5k 节点约 1MB）。
- 9 处读盘改为 `ctx.settings().await`。
- `watch.rs` 中「配置读取失败 → 30s 退避」分支删除（内存读不会失败）。
- `cmd.rs` 输出文案由「config.toml watch_* 可调」改为「改 config.toml 需
  serve --stop 重启生效」，与 `README.md:53` 对齐。
- 为纯函数 `effective_include` 补 3 个内联用例，锁定派生语义。

## 已确认的前提

`settings` **不落盘**：`store/schema.rs` 只有 nodes/subs/running/meta 四表，
`store/state.rs` 的 save/load 都不碰 settings。因此磁盘是唯一持久真源，内存
只是启动快照，**不存在「旧内存覆盖新磁盘」的风险**，风险方向相反。

## 代价（明确接受）

改配置不再热生效。此前 `watch_*`、`probe_*` 等参数可在下一个看护周期（默认
30s）自动生效，现在一律需要 `serve --stop` 重启。该能力本就残缺（`verify_url`
是启动时写进 meta 的，改 probe_url 也不会变），且 `README.md:53` 早已承诺
「需重启」。

## 后续：显式 reload（本轮未做）

`SIGHUP` 已在 `server.rs:141-147` 注册，但收到后 `select!` 直接 fall through
到 `teardown()`——**今天发 SIGHUP 等于停掉 server**。`server.rs:146` 的日志
已改为明示此语义。

实现 reload 有前置条件：**必须先加「命令执行期间禁止换入」的门禁**。否则
`cmd.rs:214` 取旧值、随后 `proxy.rs:40` 取新值，会重现同一个 bug。建议做法：
`Ctx` 增 `inflight: AtomicUsize`，`handle_conn` 进出各加减一，reload 见到
inflight > 0 则等待或拒绝；换入走 `replace_all`（`write_mu` + 原子快照）。

另需说明：`listen_addr` / `include` 变了不影响已在运行的 sing-box，只有
`probe_*`、`watch_*` 能热生效。
