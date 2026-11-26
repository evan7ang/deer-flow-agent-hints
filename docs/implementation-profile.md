# 实现画像（Agent Hint v1.0 · DeerFlow）

对应协议 §12.4 的降级画像、§7.5 的触发源登记表与 §13 的符合性分级。
基线：DeerFlow `1e19560e0`（扩展契约 API `0.2.13`，LangChain 1.3.14，
LangGraph 1.2.x，Python 3.12）。

## 能力清单（§12.1）

| 能力 | 本实现 | 说明 |
| --- | --- | --- |
| 请求出口 | ✅ | `awrap_model_call` 原地写 `request.model_settings["extra_headers"]` |
| 会话身份 | ✅ | task store 的 `TaskInfo`；lead = `thread_id`，子代理 = `{thread}:{run}:{task}` |
| 请求分类 | ✅ | 只对经过 wrap 链的主线调用发射；标题/摘要/goal/记忆/评审天然豁免 |
| 会话事实 | ✅ | 宿主库自建表（memory 后端回退 sqlite 文件） |
| 独立 HTTP 出口 | ✅ | async httpx；端点按模型实例推导，可显式覆盖 |

分级：**L1 + L2 + L3-lite**。L3 的草稿信号不实现（DeerFlow 前端不发切换/草稿事件），
归档/删除 → `stop` 通过轮询 `threads_meta` 实现。

## 降级画像（§12.4）

| 能力缺失 | 本实现 | 影响 |
| --- | --- | --- |
| 无管理出口 | 配置 `sessionControl.enabled: false` → 只做 L1 | 无 pause/resume/stop/compact |
| 无持久事件日志 | 自建事实表（宿主库/文件），重启不归零 | 无 |
| 无用户输入边界 | 每个 lead task 的首条主线调用视为轮次开局；非人类来源（schedule/extension/subagent_batch/mcp_notification）不算 | 同一 run 内的 goal 续跑不是开局 |
| 无间隔观测 | 落库“上次响应结束时刻”，无样本用 300 s | 无 |
| 无草稿信号 | **降级**：`resume` 在任务开始或下条请求前发送，不在打字时 | 预取提前量 = 任务开始 |
| 无归档事件 | 轮询 `threads_meta`（默认 3 s，可配 0 关闭） | 归档到 `stop` 有秒级延迟 |
| 无法估算前缀 | CJK 近似估算；拿不到时按“足够大”处理 | 无 |
| 宿主表不可读 | `threads_meta` 读取失败 → 不发 `stop` | 归档/删除不释放 KV |
| 隔离态无法拒绝请求 | **降级**：请求照发、不带头、WARN、状态路由可见（M-08 部分符合）；确认 KV 状态后可用 `POST /sessions/{id}/clear-isolation` 解除 | 消费端按无 hint 处理 |
| 注入自检失败 | **降级**：自检 `broken` 时整体停发（头 + 管理请求），状态路由给出原因 | 不产生“看似在工作”的假象 |
| Gateway 停机 | 不发 `stop`（线程是持久的），停止派发新 manage | 符合协议对“卸载后不再派发”的要求 |
| 非 Gateway 运行时 | 嵌入客户端 / langgraph server / ACP 不加载扩展 | 这些路径不发 hint |
| Gateway coordinator planner | 普通 `StateGraph`，不走中间件链 | 不发 hint |
| 无行 `threads_meta` 的遗留线程 | 首次见到时 `incarnation` 未知 → 无法检测删除 | 不发 `stop` |

## 触发源登记表（§7.5）

| state | 本 harness 的触发源 | 是否必需 | 缺失时的降级 |
| --- | --- | --- | --- |
| `start` / `append` | `awrap_model_call`@MODEL_LOGICAL；`start` = 首次见到 + 本 run 前无历史 + 首请求尚未被接受 | 必需 | 无 |
| `pause` | 会话无活动 run，且同一 `user_id` 下另一 lead 会话更晚被接受。在“本会话 run 结束”和“另一会话被接受”两个时点评估 | L2 | 单会话场景不发 pause（协议允许） |
| `resume` | 已 pause 的会话（任务开始时提前发，或下条请求前）；首次见到带历史的会话（首条请求前） | L2 | 仅在请求前发 |
| `stop` | 子代理 `on_task_stop`；lead 线程归档 / 删除 / `incarnation` 变化（`threads_meta` 轮询） | L2/L3 | 不发 stop |
| `compact` | 提交后的历史改写：`ThreadState.summary_text` 变化（自动压缩与手动 `/compact` 都写该通道） | L2 | 旧宿主无该通道时退化为 `on_context_compacted` 计数 |
| `parent_session_id` | 仅 branch 线程（`threads_meta.metadata_json.branch_parent_thread_id`） | 可选 | 读取失败时省略 |

## 取值来源（§6.1）

| 事实 | 来源 |
| --- | --- |
| 会话调用计数 | 事实表高水位 +1；首次见到带历史会话时以 `count(AIMessage)` 播种（不 +1，本次调用会 +1） |
| 历史改写计数 | `summary_text` 指纹变化（每次可见变化 +1）；无该通道时用 `on_context_compacted` 的 pending 计数 |
| 用户输入边界 | 每个 lead task 的首条主线调用；来源见 `deerflow_origin.kind` |
| 上次响应结束时刻 | 事实表（接受后写入） |
| 当前前缀签名 | 最终请求的 model id + 排序后的工具名 |
| 上次前缀签名 | 事实表（按逻辑调用写一次） |
| 是否带历史 | `runtime.context["__deerflow_pre_run_message_ids"]` 非空 |
| `msg_offset` | 最终请求的 `[system?] + messages`，去掉尾部空 assistant 占位 |
| 长度估算 | 系统提示 + 消息内容的 CJK 近似 |

## 有意省略的字段

| 字段 | 原因 |
| --- | --- |
| 顶层 `agent_status` / `priority` 镜像 / `prediction` / `strict_priority` | 不属于 v1.0（协议 §11：MUST NOT 混发） |
| `session.agent_type` / `session.derivation` / `cache_control.range` | 不在 v1 schema（`additionalProperties: false`） |
| `reason: background` | 不在 v1 词表；非人类 run 用 `1/critical_path` |
| 根会话 `parent_session_id` | 协议推荐省略；附录 A 的 schema 不接受显式 `null` |
| 子代理的 `parent_session_id` | 子代理上下文独立（snapshot 注入，非续接），协议 §4.2 禁止声称创建者为父 |
| `edits[].start/end` | v1 不允许 |

## 已知偏差与限制

1. **M-08 部分符合**：扩展中间件被 `IsolatedMiddleware` 包裹，钩子抛错会被吞掉并照常放行；
   隔离态只能做到“不带头放行 + WARN + 状态可见”。逃生路径：换新线程（= 新 `session_id`），
   或运维确认后调 `POST /api/extension/agent-hints/sessions/{id}/clear-isolation`。
   注意只有 manage 的 **5xx/超时/连接中断** 才进入隔离；普通网关对占位 body 的 4xx 归为
   `failed`，不隔离、不重试。
2. **两次调用之间发生多次改写的合并**：`summary_text` 是指纹而不是计数器，多次改写只 +1；
   仅在无该通道的旧宿主上按事件计数。
3. **中段改写不计 epoch**：`POST /state`、工具提醒/动态上下文的中段增删不算改写；若头部
   消息 id 变化，会额外抑制当轮 `cache_control`（宁可少发）。
4. **`call_index` 播种是近似**：`count(AIMessage)` 受压缩删消息与错误兜底消息影响；只用于
   首次看到既有线程时的起点，之后严格单调。
5. **barrier 有界**：在途 mutation 最多等 `pendingOpAwaitMs`（默认 3 s）；超时后放行。
6. **provider 门**：仅 `BaseChatOpenAI` 族（含 DeerFlow 的 Guarded/Patched/MindIE/Vllm 等）
   与 `ChatAnthropic` 族注入。`CodexChatModel`（裸 httpx 自建头）、Google 族
   （`additional_headers`）、Ollama 不注入。
7. **`stop` 不覆盖进程退出**：线程是持久实体，退出不等于会话结束；停机只停止派发。

## 证据

| 证据 | 位置 |
| --- | --- |
| 单元测试（wire/policy/config/control） | `tests/`，`pytest` |
| 真实扩展宿主集成（真实 `IsolatedMiddleware` + 锚点栈 + 抓包模型） | `tests/test_integration.py` |
| 真 Gateway + 桩上游 e2e（场景 A/B/C/D + 归档） | `bash e2e/run.sh` |
| 脱敏抓包 | `docs/evidence/e2e-capture-20261126.jsonl` |
| 打包检查 | `check_extension.py`（见任务文档） |
