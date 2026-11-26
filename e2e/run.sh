#!/usr/bin/env bash
# End-to-end verification: real Gateway + scripted upstream, asserting on the
# bytes that leave the process. Requires a DeerFlow backend environment.
set -euo pipefail
cd "$(dirname "$0")/.."

BACKEND=${DEERFLOW_BACKEND:-${HOME}/deer-flow/backend}
PY=${DEERFLOW_PY:-${BACKEND}/.venv/bin/python}
if [[ ! -x "${PY}" ]]; then
  echo "✗ DeerFlow venv python not found: ${PY} (set DEERFLOW_PY)" >&2
  exit 1
fi

export PYTHONPATH="src:${BACKEND}:${PYTHONPATH:-}"
export PYTHONDONTWRITEBYTECODE=1
exec "${PY}" e2e/run_e2e.py "$@"
