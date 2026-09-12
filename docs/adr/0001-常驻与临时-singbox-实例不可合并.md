# ADR-0001：常驻与临时 sing-box 实例不可合并

- 状态：已接受
- 日期：2026-09-12

## 背景

项目通过 spawn 外部二进制 `sing-box run -c <config>` 起本地代理。相同的
「写配置 → spawn → 等端口 → 杀 → 删文件」链出现在三处：

- `src/proxy.rs:29 launch_pairs` — 常驻实例，写 `running` 表
- `src/tester.rs:283 probe_single_node` — 临时探测实例，不写表
- `src/cmd.rs:263 stop_inner` / `src/server.rs` teardown — 反向清理

架构评审曾提出「塌进一个 supervisor 深模块」。经逐项核对，**否决该提议**。

## 决策

三处差异是**有意设计**，不是抄漏。保留两套实例模式，只抽公共原语。

| 维度 | 常驻 | 临时 | 若强行统一 |
|---|---|---|---|
| 脱离进程组 | `process_group(0)`（`proxy.rs:92`） | 不脱离（`tester.rs:338`） | 都脱离 → probe 中断留下最多 32 个孤儿；都不脱离 → server 退出连坐常驻代理，re-adopt 失效 |
| listen | `settings.listen_addr`，默认 `0.0.0.0` | 硬编码 `127.0.0.1`（`tester.rs:310`） | 透传 → 32 个临时实例暴露内网 |
| include 策略 | 派生追加 probe_url host（`proxy.rs:13`） | 必须空 | 透传 → 探测流量落 final 直连，结果失真 |
| 清理粒度 | 删配置留日志（备查） | 全删（每节点一对文件，留会撑爆） | — |
| 失败表达 | `pid=0` 哨兵（未装 sing-box） | 全 false 的 `ProbeResult` | 压平 → 「无二进制」被当成「节点不可用」→ 整批误标死 |

## 影响

可安全抽取的公共原语只有四个：写配置、`wait_for_ports`、kill+wait、删文件。
差异项必须参数化，且每个模式固定取值。

顺带记录两个**只读原语层面**、与本次决策无关的真漏，可独立修复：

- `tester.rs:303` 的 TOCTOU：`pick_free_port` 释放 listener 后到 sing-box bind
  之间端口可被外部进程抢走，导致探测打在别人监听上、污染整批。
  `proxy.rs:56` 的 `are_ports_free` 正是防这个，探测路径没有。
- `proxy.rs:96`：spawn 失败时只删 config，遗留 `.log`。

## 若日后要重新提议合并

必须先说明这五个维度各自如何参数化，以及为什么「统一」不会在四个调用点中
的某一个上引入上表右列的功能级破坏。
