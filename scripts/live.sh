#!/usr/bin/env bash
# Live demo wrapper: picks the DeerFlow venv python and exposes the plugin src.
set -euo pipefail
cd "$(dirname "$0")/.."

BACKEND=${DEERFLOW_BACKEND:-${HOME}/deer-flow/backend}
PY=${DEERFLOW_PY:-${BACKEND}/.venv/bin/python}
if [[ ! -x "${PY}" ]]; then
  echo "✗ DeerFlow venv python not found: ${PY} (set DEERFLOW_PY)" >&2
  exit 1
fi

export DEERFLOW_BACKEND="${BACKEND}"
export PYTHONPATH="src:${BACKEND}:${PYTHONPATH:-}"
export PYTHONDONTWRITEBYTECODE=1
exec "${PY}" scripts/live_demo.py "$@"
