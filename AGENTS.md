# 仓库开发规范

本文件适用于整个仓库。协议契约是父目录的
[`../agent-hint-protocol.md`](../agent-hint-protocol.md)（v1.0，2026-11-25）；
本仓库只实现 DeerFlow 侧的**发送方**（消费端不在范围内）。

## 分层

```text
wire.py / policy.py   纯函数，只用 stdlib，可脱离 DeerFlow 单测
config.py             配置解析，install 期报错
store.py              会话事实：宿主库自建表 + sqlite 文件回退
control.py            manage 传输：async、每会话串行、仅 resume 重试、隔离
threads.py            threads_meta 只读（list 驱动），失败即降级
runtime.py            事实状态机；不 import deerflow 私有模块（lazy import）
hooks.py / lifecycle.py / service.py / router.py / trace.py   宿主接线
```

## 命令

```bash
PYTHONPATH=src:tests ~/deer-flow/backend/.venv/bin/python -m pytest tests -q
bash e2e/run.sh
uv run --project ~/deer-flow/backend --no-sync python ~/deer-flow/scripts/check_extension.py .
```

## 硬性规则

1. **body 零 hint**：hint 只经 `X-Agent-Hint` 头发射；`extra_headers` 为增量合并。
2. **严格 v1.0**：顶层只有四块；不发 `agent_status`、顶层 `priority` 镜像、
   `prediction`、`strict_priority`、`session.agent_type`、`cache_control.range`。
   根会话省略 `parent_session_id`。
3. **原地注入**：只改 `request.model_settings` 里的字典；不要 `request.override`
   后交给 handler —— DeerFlow 的隔离包装会丢弃替换后的请求。
4. **fail-open**：钩子里任何异常都必须吞掉并记 WARN，模型请求照常发出；
   唯一例外是协议允许的“不带头”降级。
5. **异步 I/O**：钩子路径上不得同步阻塞（DeerFlow 有 `make test-blocking-io`）。
6. **事实以会话为键**：唯一可写者是该线程当前 run 的 worker；跨进程只通过
   窄字段更新（`paused` / `stopped` / `quarantined_reason` / `pending_rewrites`）。
7. **改写只认真实信号**：`summary_text` 通道变化（自动或手动压缩）；不要用
   消息数量或首条内容做“猜测式” epoch 自增。
8. **辅助请求不记账**：标题、摘要、goal、记忆、评审调用天然不经过 wrap 链；
   不要给它们注入任何 hint。
9. **无内容 trace**：trace 只记 hint 与结果分类，不记端点、凭据、消息内容。
10. 改 wire 形状 / 触发源 / 状态机必须同步 `docs/implementation-profile.md`、
    `docs/conformance.md` 与单测向量（协议附录 B 的 V1–V10）。

## 变更检查

- 改动 hook 或状态机：`pytest tests -q` + `bash e2e/run.sh`。
- 改动查询/建表：跑集成测试（宿主库路径）与文件回退路径。
- 改动打包：跑 `check_extension.py`。
