#!/usr/bin/env bash
# Local (no Docker) environments for run_matrix.sh / run_http_matrix.sh. Needs uv (https://docs.astral.sh/uv/).
set -euo pipefail
cd "$(dirname "$0")"
PKGS="greenlet flask waitress fastapi uvicorn uvloop httptools"
uv venv -q -p 3.14t .venv-ft && uv pip install -q -p .venv-ft/bin/python $PKGS
uv venv -q -p 3.14  .venv-gil && uv pip install -q -p .venv-gil/bin/python $PKGS
.venv-ft/bin/python -c "import sys, fastapi, uvicorn, uvloop, httptools, flask, waitress, greenlet; print('free-threaded ok, gil_enabled =', sys._is_gil_enabled())"
