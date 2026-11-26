"""Operator routes: status, live hint feed, and the isolation escape hatch.

All read-only except ``clear-isolation``; the host applies its own auth to
``/api/*``. The live page is a single self-contained HTML document so it works
behind the Gateway's nginx with no extra assets.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

from deerflow_agent_hints.service import AgentHintsService

LIVE_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Agent Hints · live</title>
<style>
:root { color-scheme: dark; }
body { margin:0; font:13px/1.45 ui-monospace,SFMono-Regular,Menlo,monospace; background:#0b0f14; color:#d8e1ea; }
header { position:sticky; top:0; z-index:2; background:#0b0f14ee; padding:10px 16px;
  border-bottom:1px solid #1d2733; display:flex; gap:16px; align-items:baseline; flex-wrap:wrap; }
h1 { font-size:14px; margin:0; font-weight:600; letter-spacing:.04em; }
#meta { color:#8fa3b8; }
#meta b { color:#d8e1ea; font-weight:600; }
table { width:100%; border-collapse:collapse; }
th, td { padding:4px 8px; text-align:left; white-space:nowrap; border-bottom:1px solid #131a23; vertical-align:top; }
th { position:sticky; top:41px; z-index:1; background:#0b0f14f2; color:#7d90a5; font-weight:500; }
td.num { text-align:right; }
tr:hover { background:#101720; }
.badge { display:inline-block; padding:0 6px; border-radius:4px; font-size:11px; font-weight:600; }
.start { background:#123a2a; color:#6fe0a8; }
.append { background:#12283a; color:#79b8ff; }
.pause { background:#3a2c12; color:#ffcc66; }
.resume { background:#12363a; color:#66e0e0; }
.stop { background:#3a1212; color:#ff8080; }
.compact { background:#2a123a; color:#c48cff; }
.other { background:#1b2430; color:#9fb3c8; }
details summary { cursor:pointer; color:#6f8399; }
pre { margin:4px 0 0; white-space:pre-wrap; color:#b9c9d8; }
label { color:#8fa3b8; user-select:none; }
</style>
</head>
<body>
<header>
  <h1>X-Agent-Hint · live</h1>
  <div id="meta">connecting…</div>
  <label><input type="checkbox" id="internal"> show internal events</label>
</header>
<table>
<thead><tr><th>time</th><th>kind</th><th>session</th><th>call</th><th>epoch</th><th>state</th>
<th>priority</th><th>ttl_s</th><th>msg_offset</th><th>outcome</th><th>hint</th></tr></thead>
<tbody id="rows"></tbody>
</table>
<script>
const rows = document.getElementById('rows');
let since = 0, autoScroll = true;
window.addEventListener('scroll', () => {
  autoScroll = (window.innerHeight + window.scrollY) >= document.body.scrollHeight - 40;
});
document.getElementById('internal').addEventListener('change', () => { rows.innerHTML = ''; since = 0; });
const esc = (v) => String(v ?? '').replace(/[&<>"]/g, (c) => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
function badge(state) {
  const cls = ['start','append','pause','resume','stop','compact'].includes(state) ? state : 'other';
  return '<span class="badge ' + cls + '">' + esc(state) + '</span>';
}
function add(e) {
  if (!document.getElementById('internal').checked && !['hint','manage'].includes(e.event)) return;
  const h = e.hint || {};
  const s = h.session || {};
  const prio = h.priority_control ? h.priority_control.priority.level + '/' + h.priority_control.priority.reason : '';
  const cache = h.cache_control || {};
  const state = s.state || e.state || e.verb || '';
  const tr = document.createElement('tr');
  tr.innerHTML =
    '<td class="num">' + new Date(e.at_ms).toLocaleTimeString() + '</td>' +
    '<td>' + esc(e.event) + '</td>' +
    '<td>' + esc((s.session_id || e.session_id || '').slice(0, 8)) + '</td>' +
    '<td class="num">' + esc(s.call_index ?? e.call_index ?? '') + '</td>' +
    '<td class="num">' + esc(s.epoch ?? e.epoch ?? '') + '</td>' +
    '<td>' + (state ? badge(state) : '') + '</td>' +
    '<td>' + esc(prio) + '</td>' +
    '<td class="num">' + esc(cache.ttl_s ?? '') + '</td>' +
    '<td class="num">' + esc(cache.msg_offset ?? '') + '</td>' +
    '<td>' + esc(e.outcome ?? '') + '</td>' +
    '<td>' + (h.session ? '<details><summary>json</summary><pre>' + esc(JSON.stringify(h)) + '</pre></details>' : '') + '</td>';
  rows.appendChild(tr);
  if (autoScroll) window.scrollTo(0, document.body.scrollHeight);
}
async function poll() {
  try {
    const r = await fetch('events?since=' + since, {cache: 'no-store'});
    const d = await r.json();
    since = Math.max(since, d.seq || 0);
    (d.events || []).forEach(add);
  } catch (err) {}
}
async function meta() {
  try {
    const r = await fetch('status', {cache: 'no-store'});
    const d = await r.json();
    const c = d.counters || {}, m = d.manage || {};
    document.getElementById('meta').innerHTML =
      'injection <b>' + esc(d.injection) + '</b> · level <b>' + esc(d.level) + '</b>' +
      ' · sessions <b>' + esc(d.tracked_sessions ?? 0) + '</b>' +
      ' · start/append <b>' + esc(c.state_start || 0) + '/' + esc(c.state_append || 0) + '</b>' +
      ' · cache <b>' + esc(c.cache_declared || 0) + '</b>' +
      ' · manage <b>' + esc(JSON.stringify(m)) + '</b>' +
      ((d.quarantined || []).length ? ' · <b style="color:#ff8080">quarantined ' + d.quarantined.length + '</b>' : '');
  } catch (err) {}
}
poll(); meta();
setInterval(poll, 500); setInterval(meta, 2000);
</script>
</body></html>"""


def build_router(service: AgentHintsService) -> APIRouter:
    router = APIRouter(prefix="/api/extension/agent-hints", tags=["agent-hints"])

    @router.get("/status")
    async def read_status() -> dict[str, Any]:
        return await service.status()

    @router.get("/events")
    async def read_events(since: int = 0, limit: int = 200) -> dict[str, Any]:
        return service.events(since=max(0, since), limit=max(1, min(500, limit)))

    @router.get("/live", response_class=HTMLResponse)
    async def live_view() -> str:
        return LIVE_HTML

    @router.post("/sessions/{session_id}/clear-isolation")
    async def clear_isolation(session_id: str) -> dict[str, Any]:
        """Forget an isolation record after an operator confirmed the KV state."""
        cleared = await service.clear_isolation(session_id)
        return {"session_id": session_id, "cleared": cleared}

    return router
