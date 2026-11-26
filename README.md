# deerflow-agent-hints

[Agent Hint protocol v1.0](../agent-hint-protocol.md) sender for
[DeerFlow](https://github.com/bytedance/deer-flow), packaged as a DeerFlow
extension.

Every main-line model request carries one `X-Agent-Hint` header (session
identity, lifecycle state, priority, cache intent). The session lifecycle is
driven with dedicated manage requests (`pause` / `resume` / `stop` / `compact`)
whose body is a placeholder. The model-visible request body is never touched.

- Conformance target: **L1 + L2 + L3-lite** — headers, all four lifecycle
  verbs, archive/delete → `stop` via `threads_meta` polling. No draft/focus
  signal (DeerFlow publishes none).
- Baseline: DeerFlow `1e19560e0` (extension API `0.2.13`, LangChain 1.3.14).
- Wire: strict protocol v1.0 — four top-level blocks only; the legacy
  `agent_status` / `prediction` / `strict_priority` vocabulary is not emitted.
- Implementation profile: [`docs/implementation-profile.md`](docs/implementation-profile.md).
- Conformance checklist: [`docs/conformance.md`](docs/conformance.md).

```text
src/deerflow_agent_hints/
├── __init__.py     # @extension(api="0.2.13") install(): the only entry point
├── wire.py         # four-block wire objects, verb -> edit, base64url (pure stdlib)
├── policy.py       # §6 derivations: TTL, msg_offset, signatures, rewrite detection
├── config.py       # plugin config parsing (fails at install time)
├── store.py        # per-session facts: host DB table + SQLite-file fallback
├── control.py       # async manage transport: per-session serialization, isolation
├── runtime.py      # fact machine, identity resolution, priorities, sweeps
├── hooks.py        # MODEL_LOGICAL + MODEL_PHYSICAL middlewares
├── lifecycle.py    # on_task_start/stop, on_context_compacted
├── service.py       # resources, injection self-check, threads_meta poller
├── threads.py      # read-only threads_meta access (archive/delete/branch)
├── router.py       # GET /api/extension/agent-hints/status
└── trace.py        # content-free JSONL diagnostics
```

## Install

Develop against a DeerFlow checkout (editable, no lock changes):

```bash
uv pip install --python <deer-flow>/backend/.venv/bin/python -e .
```

Then enable it in `config.yaml` (gitignored in DeerFlow) and restart the Gateway:

```yaml
plugins:
  - name: agent-hints
    package: deerflow-extension-agent-hints
    use: deerflow_agent_hints:install
    enabled: true
    required: false
    table_prefix: extension_agent_hints_
    config:
      enabled: true
      priority: dynamic
      cacheControl: dynamic
      sessionControl:
        enabled: true
        pendingOpAwaitMs: 3000
        controlTimeoutMs: 2000
        retries: 1
        threadPollSeconds: 3
      state:
        path: .deer-flow/agent-hints.db
      trace:
        enabled: false
        file: null
```

The production install path is `make extension-install SOURCE=<abs path>`; local
directories are installed as snapshots and that transaction edits
`backend/pyproject.toml` and `uv.lock`.

## What goes on the wire

```http
POST /v1/chat/completions
X-Agent-Hint: eyJzZXNzaW9uIjp7InNlc3Npb25faWQiOi...
content-type: application/json

{ "model": "...", "messages": [ ... ], "stream": true }   ← unchanged
```

A decoded hint (fresh session, first call):

```json
{
  "session": { "session_id": "0f0c...", "call_index": 1, "epoch": 0, "state": "start" },
  "priority_control": { "priority": { "level": 3, "reason": "user_waiting" } },
  "cache_control": { "type": "ephemeral", "ttl_s": 600, "msg_offset": 12 }
}
```

A manage request (`pause` shown) posts `{"model":"…","messages":[],"stream":false}`
with `session.state = "pause"` and
`context_management = {"manage_request": true, "edits": [{"type": "offload", "target": "session"}]}`.

## Lifecycle triggers

| state | Trigger |
| --- | --- |
| `start` | First accepted main-line call of a brand-new session (no pre-run history) |
| `append` | Every other main-line call |
| `pause` | A session is idle (no active run) and another lead session of the same user was accepted more recently |
| `resume` | Before the first request of an adopted/restored/forked session, and before the next request of a paused one |
| `compact` | A committed rewrite: DeerFlow's `summary_text` channel changed (automatic or manual `/compact`) |
| `stop` | A subagent execution ended; a lead thread was archived, deleted, or recreated |

`start`/`append` ride on the ordinary request. `pause`/`resume`/`stop`/`compact`
never do — they are separate manage requests.

## Configuration

Validated once at install time; an invalid option fails the plugin load instead
of degrading silently (`required: false` then fails open with a diagnostic).

| option | default | meaning |
| --- | --- | --- |
| `enabled` | `true` | Master switch |
| `priority` | `"dynamic"` | `"omit"`, `"dynamic"`, or a fixed `{level, reason}` |
| `cacheControl` | `"dynamic"` | `"omit"`, `"dynamic"`, or a fixed `{ttl_s, msg_offset?}` |
| `models` | `[]` (all) | Model-name globs that get hints |
| `sessionControl.enabled` | `true` | Send manage requests |
| `sessionControl.pendingOpAwaitMs` | `3000` | Request-side barrier for an in-flight mutation |
| `sessionControl.controlTimeoutMs` | `2000` | Per-attempt manage timeout |
| `sessionControl.retries` | `1` | Extra attempts, `resume` only |
| `sessionControl.threadPollSeconds` | `3` | `threads_meta` poll interval; `0` disables |
| `controlEndpoint` | derived | `{url, apiKey?, model?, headers?}` manage override |
| `state.path` | `.deer-flow/agent-hints.db` | Fact store when the host has no database |
| `trace.enabled` / `trace.file` | `false` / stdout-off | Content-free JSONL diagnostics |

## Verification

```bash
# unit + in-process integration (needs a DeerFlow backend environment)
PYTHONPATH=src:tests <deer-flow>/backend/.venv/bin/python -m pytest tests -q

# end-to-end: real Gateway + scripted upstream, asserting on captured bytes
bash e2e/run.sh
```

`e2e/run.sh` boots the real Gateway in-process with an isolated `DEER_FLOW_HOME`,
points a model at a local scripted upstream, and runs the protocol scenarios:
header shape and `msg_offset`, `start`/`append`, auxiliary exemption,
pause/resume across threads, compaction → `compact` + epoch bump + cache
suppression, archive → `stop`, and the status route.

## Known limits

- The header is written by mutating `request.model_settings` in place: DeerFlow's
  isolated extension wrapper drops a replaced request. A startup self-check
  proves the pass-through; when it reports `broken`, all emission (hints and
  manage traffic) is suspended and the status route says why, instead of
  pretending to work.
- A session isolated by an unknown manage result cannot reject new requests
  (extension hooks fail open); it proceeds unhinted with a warning. After
  confirming the KV state, an operator can lift it with
  `POST /api/extension/agent-hints/sessions/{id}/clear-isolation`.
- A `pause` is only remembered once the endpoint accepted it: a plain gateway
  answering 4xx for the placeholder body therefore does not trigger a useless
  `resume` before every later request.
- Only the Gateway runtime is covered; the embedded client, LangGraph server and
  ACP paths do not load extensions.
- `threads_meta` is read through the legacy `session_factory`; every failure
  degrades to "no `stop`".
