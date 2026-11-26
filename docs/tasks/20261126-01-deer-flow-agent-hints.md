# 任务：DeerFlow Agent Hint 插件首版（L1 + L2 + L3-lite）

- 日期：2026-11-26
- 设计：[`20261126-01-deer-flow-agent-hints.md`](../design/20261126-01-deer-flow-agent-hints.md)
- 协议：`agent-hint-protocol.md` v1.0

## 目标

在 DeerFlow 上以**扩展**形式实现 Agent Hint v1.0 发送方：主线请求带
`X-Agent-Hint`，生命周期动词走独立 manage 请求，归档/删除发 `stop`。

## 实现路径

1. 纯函数核心：wire 四块、§6 取值算法、配置校验。
2. 事实表：宿主库自建表（`extension_agent_hints_sessions`）+ 文件回退。
3. 两个中间件：`MODEL_LOGICAL` 开逻辑调用；`MODEL_PHYSICAL` 原地写头。
4. 生命周期：`on_task_start/stop`、`on_context_compacted`；`summary_text` 改写检测。
5. 管理通道：async httpx、每会话串行、resume 有界重试、未知结果隔离。
6. 轮询：`threads_meta` 归档/删除/重建 → `stop`（每会话一次）。
7. 运营面：注入自检、状态路由、无内容 trace。

## 验收标准与结果

| 验收 | 命令 | 结果 |
| --- | --- | --- |
| 静态检查 | `ruff check src tests e2e` / `ruff format --check` | 通过 |
| 单元 + 集成 | `PYTHONPATH=src:tests <deerflow venv>/python -m pytest tests -q` | 58 passed |
| e2e（真 Gateway + 桩上游） | `bash e2e/run.sh` | PASS（9 组断言，见下） |
| 打包检查 | `uv run --project ~/deer-flow/backend --no-sync python ~/deer-flow/scripts/check_extension.py .` | 通过 |
| 真实扩展宿主 | `tests/test_integration.py`：真实 `IsolatedMiddleware` + 锚点栈 + 抓包模型 | 7 passed |

e2e 断言（原文输出见 `docs/evidence/`）：

1. 状态路由 200，注入自检 `ok`，等级 `L1+L2+L3-lite`。
2. 首次 run：`start`、`call_index=1`、`epoch=0`、`3/user_waiting`、单个 `ephemeral`、
   `msg_offset == wire messages`、body 零 hint。
3. 第二个 run：`append`、`call_index=2`。
4. 第二个线程被接受 → 线程一收到 `pause`（offload）。
5. 回到线程一 → `resume` 先于请求被接受，`call_index` 等于即将的调用。
6. 触发压缩：摘要请求无 hint；`compact` 携带旧 epoch；下一条请求 `epoch+1` 且抑制 cache。
7. 归档线程二 → 恰好一条 `stop`。
8. 状态路由报告活跃会话、无隔离会话。
9. 全量抓包不变量：普通/管理 hint 全部通过 v1.0 校验；无辅助请求带 hint；body 无 hint 字段。

## 验证记录

```text
$ PYTHONPATH=src:tests ~/deer-flow/backend/.venv/bin/python -m pytest tests -q
.........................................................                [100%]

$ bash e2e/run.sh
[1] first run: start + header shape + msg_offset
  ok   main-line request carries X-Agent-Hint
  ok   hint conforms to v1.0: []
  ok   session.state == start (start)
  ok   call_index == 1 (1)
  ok   epoch == 0 (0)
  ok   priority == 3/user_waiting
  ok   msg_offset 2 == wire messages 2
  ok   body carries no hint field (['messages', 'model', 'stream', 'tools'])
...
[6] compaction: auxiliary summary, compact manage, epoch bump, cache suppressed
  ok   compact carries old epoch 0, request carries 1
  ok   cache declaration suppressed on the rewrite round
[7] archive -> stop (exactly once)
  ok   exactly one stop for the archived thread (1)
[9] whole-capture invariants
  ok   all 6 ordinary hints conform
  ok   all 7 manage hints conform
PASS
```

## 剩余限制

见 [实现画像](../implementation-profile.md) 的“已知偏差与限制”。要点：隔离态无法拒绝请求
（M-08 部分）、同间隙多次改写合并为一次 epoch、`threads_meta` 为 legacy 读取、
非 Gateway/coordinator 路径不覆盖、多 worker 未做真机验证。
