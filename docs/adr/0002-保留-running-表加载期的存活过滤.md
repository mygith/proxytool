# ADR-0002：保留 running 表加载期的存活过滤

- 状态：已接受
- 日期：2026-09-12

## 背景

`src/store/state.rs:174`：

```rust
st.running.retain(|r| crate::run::is_pid_alive(r.pid));
```

加载时丢弃「进程已死」的运行行。架构评审曾指出两点并提出删除：

1. 持久化层反向依赖进程层（store → run，读 `/proc`）；
2. `proxy.rs:201-217 rollback_mapping` 会补写 **pid=0** 的运行行，注释称
   「pid 记 0（无进程，后续 stop/launch 以端口为准）」，看起来与 retain 冲突。

## 决策

**保留该行。**

## 理由

### 1. 所谓「自相矛盾」不成立

`retain` 只在 load 时执行。本进程内 pid=0 的行一直可见，`rollback_mapping`
的注释语义在其生命周期内成立。真正的代价仅是：**跨进程重启后这些恢复行蒸发**，
看护无法据以恢复。这是「能力缺失」，不是「写入即失效」，价值远低于初判。

### 2. 删除后的回归面远大于收益

必须同步修改约 5 处：

- `watch.rs:44 adopt_running` — **最危险的静默改动**：会给死 pid / pid=0
  的端口恢复看护，`watch.rs:113` 随即 failover 重拉。用户手动 `kill -9`
  的端口将在下次 `serve` 启动时**被复活**。
- `main.rs:293-306` status — 开始显示已退出的进程。
- `cmd.rs:263 stop_inner`、`cmd.rs:322 switch_cmd` — 端口集合扩大（经
  `select.rs:307 running_ports`），`--all` 连带操作此前不可见的死行。
- `flow.rs:214` prune 的 `in_use` 扩大 — 少删节点。
- `proxy.rs:205` 分支翻转：死行存在时走 `set_running_node` 而不再补 pid=0，
  与 `proxy.rs:197-200`「pid 必须是真正占端口的进程」相冲突。

另有 2 个测试需要改：`store::tests::test_load_filters_dead_running_entries`
会红，必须删除或反转断言。

附带问题：死行不再被 `delete_missing` 回收，会长期累积。

## 影响

依赖方向（store → run）这一条不变量确实被违反，接受它。理由：它是唯一一处，
且换来的收益（加载即得到「实际在跑」的运行态）被 5 处调用方依赖。

## 若日后要重新提议删除

必须先解决 `adopt_running` 的复活语义。可选方案：由 kill/stop 路径写一条
「有意停止」标记，而不是靠 pid 死亡来推断——但这属于独立变更，需要新增字段。
