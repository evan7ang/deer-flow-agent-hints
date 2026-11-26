# DeerFlow Agent Hint 插件：设计与决策

- 日期：2026-11-26
- 协议：[`agent-hint-protocol.md`](../../../agent-hint-protocol.md) v1.0（2026-11-25）
- 目标：L1 + L2 + L3-lite；DeerFlow `1e19560e0`（扩展 API 0.2.13，LangChain 1.3.14）
- 复用来源：[`langchain-agent-hints@9d9f662`](https://github.com/evan7ang/langchain-agent-hints)（v0.5.0）
- 参考实现：[`dsh-agent-hints`](https://github.com/evan7ang/dsh-agent-hints)

## 1. 已核实的 DeerFlow 事实

全部用源码阅读 + 真实宿主探针确认（探针只在内存里跑，未改宿主）。

| 事实 | 结论 | 影响 |
| --- | --- | --- |
| 扩展中间件包装 | `IsolatedMiddleware` 只把**原始 request** 传给下游（`extensions/isolation.py:325`），钩子返回的替换请求被丢弃 | 必须**原地**写 `request.model_settings`，不能 `request.override` 后交给 handler |
| 原地写能否上线 | 真实 `IsolatedMiddleware` + `create_agent` 探针：头到达模型 kwargs；`GuardedChatOpenAI` 在 `/chat/completions` 与 `/responses` 都上线；`ClaudeChatModel` 到达 `messages.create` | D1 成立；用启动自检兜底 |
| 挂点语义 | `MODEL_LOGICAL` 在重试外层；`MODEL_PHYSICAL` 在请求变换最内层；`before_model` 是独立图节点（压缩在 wrap 链外完成） | 逻辑调用计数与最终请求度量分开 |
| 会话身份 | `TaskInfo(task_id, run_id, thread_id, kind, parent_task_id, agent_name)` 由宿主 seed 进 task store | 身份来源；子代理与 lead 可区分 |
| 是否带历史 | `runtime.context["__deerflow_pre_run_message_ids"]` 是本 run 前的消息 id 集合 | `start`/`append` 与 resume 的可靠信号 |
| 历史改写 | `ThreadState.summary_text` 只在压缩（自动/手动 `/compact`）后变化 | 确定性的改写信号，不用消息计数猜测 |
| 消息头稳定性 | 有 checkpointer 时消息 id 跨 run 稳定；无 checkpointer 时 ids 每次新建 | 头部指纹只做“抑制缓存”的安全网，不做 epoch |
| 提醒消息 churn | `TodoMiddleware` 等会对状态发 `RemoveMessage` 再重加（`todo_middleware.py:658`） | 禁止用“整段 id 前缀相等”判改写 |
| 运行来源 | `TaskInfo.metadata["deerflow_origin"].kind ∈ {im_channel, github, schedule, extension, subagent_batch, mcp_notification}`；缺失 = interactive/legacy | 轮次开局与优先级 |
| goal 续跑 | 同一 run 内的隐藏轮次，不触发新的 `on_task_start` | 续跑不是轮次开局 |
| 子代理上下文 | 每次执行新建，绝不复用；`task_id` 唯一；同任务结束即弃 | 子代理 = 独立会话；结束时 `stop` |
| 扩展持久化 | 扩展自建 `MetaData` + 表；`session_factory` 在 memory 后端为 `None`；`table_prefix` 声明给 alembic 过滤 | 事实表设计 |
| `threads_meta` | `deerflow_archived`、`branch_parent_thread_id`、`incarnation`、`status`；无批量按 id 的正式 API | 归档/删除/血缘检测；legacy 接口自担风险 |
| 前端 | 切换线程只是路由跳转；`/read` 有防抖且已读线程不发 | 无草稿/焦点信号，L3 只做归档 |
| 钩子预算 | `on_task_start/stop` 共享 3 s；压缩观察者是 fire-and-forget | 钩子里不做重活 |
| 多 worker | 宿主保证“一个线程同时只有一个 run”；钩子在各自 worker 进程内 | 事实落库保证跨 worker 的 pause/计数正确 |
| 模型族 | `BaseChatOpenAI` 族与 `ChatAnthropic` 族支持 per-request headers；Codex 自建头、Google 走 `additional_headers` | provider 门 |

## 2. 决策记录

| # | 决策 | 理由 |
| --- | --- | --- |
| D1 | 纯扩展 + 原地写 `model_settings` | 零核心改动；如实测可用；`required: false` + 启动自检把“宿主关掉该行为”的风险变成可见故障 |
| D2 | 独立仓库 + 移植，而不是依赖 langchain-agent-hints | 挂载约束、身份、改写信号、异步传输、端点解析全部不同；可复用的是纯函数（codec、token 估算、逻辑条数）与 manage 可靠性语义 |
| D3 | wire 严格按 v1.0 | 用户指定的协议；langchain 仓的“被容忍超集字段”在严格校验下不合法 |
| D4 | `start`/`append` 用 `__deerflow_pre_run_message_ids` | 比数消息可靠；区分新线程与恢复/fork |
| D5 | `epoch` 只认 `summary_text` 变化 | 确定性、同步、覆盖自动与手动压缩；避免 Todo/日期提醒造成的伪改写 |
| D6 | 头部 id 指纹只抑制缓存 | 中段 churn 不影响 epoch；宁可少发缓存声明 |
| D7 | 事实表放宿主库（自建表），memory 后端回退 sqlite 文件 | 重启不归零、跨 worker pause 规则成立；遵循扩展持久化惯例 |
| D8 | `pause` = 空闲 + 同用户另一 lead 会话更晚被接受 | 协议 V5 的推广；两个时点都评估，覆盖并发 run；按 `user_id` 隔离避免多用户互扰 |
| D9 | `stop` = 子代理结束 + lead 归档/删除/重建 | 子代理上下文真实销毁；线程是持久实体，轮次结束不是 stop |
| D10 | 屏障 = 有界等待 + 隔离态不带头放行 | 隔离包装吞掉钩子异常，纯扩展无法 fail-closed；如实记录 M-08 偏差 |
| D11 | manage 端点按模型实例推导，凭据只在内存 | 与 opencode D3 一致；poller 场景用 `app_config` 重建 |
| D12 | 不实现草稿/焦点信号 | 前端无信号；改前端就不再是纯插件 |
| D13 | 文档双语（README 中文为主） | 与同系列仓库一致 |

## 3. 架构

```text
on_task_start/stop ──┐
on_context_compacted ─┤
threads_meta 轮询 ────┼─▶ SessionStore(宿主库/文件) ──▶ ControlService(每会话串行队列) ──▶ manage 请求
LogicalHint@MODEL_LOGICAL ─┘        │
        └─ 打开逻辑调用 ─▶ HintBuilder(纯函数) ─▶ PhysicalHint@MODEL_PHYSICAL ─原地写 extra_headers─▶ 线上
```

- 纯函数：`wire.py` / `policy.py`（可脱离 DeerFlow 单测）。
- 状态：`store.py` 一表一会话；`runtime.py` 状态机（写穿缓存 + 窄字段更新）。
- 传输：`control.py` 只做 HTTP 与结果分类；超时/重试/隔离与协议条款一一对应。
- 宿主接线：`hooks.py` / `lifecycle.py` / `service.py` / `router.py`。

## 4. 里程碑与状态

| 里程碑 | 内容 | 状态 |
| --- | --- | --- |
| M0 | 脚手架、wire/policy/config、单测 | 完成 |
| M1 | L1 注入 + 身份/事实存储 + 启动自检 + 集成测试 | 完成 |
| M2 | L2 管理请求、屏障与隔离、resume/compact/pause、子代理 stop | 完成 |
| M3 | L3-lite：`threads_meta` 轮询、branch 父 id、状态路由、trace | 完成 |
| M4 | 真 Gateway e2e（A–D + 归档）、打包检查、实现画像/符合性/证据 | 完成 |

## 5. 风险与未验证项

- 原地写依赖宿主“透传同一 request 对象”的未文档化行为；自检 + 集成测试 + 版本标记兜底。
- 读 `threads_meta` 走 legacy `session_factory`；表/列变化时自动降级为不发 `stop`。
- 手动 `/compact` 的 epoch 变化由 `summary_text` 覆盖（设计保证，e2e 验证的是自动压缩路径）。
- 未在真实推理网关（Motor/vLLM）上验证 KV 动作；e2e 只验证发送侧字节。
- 多 worker 部署未做真机验证；事实表与 `claim_stop` 的时间戳/条件更新按单写者模型设计。
