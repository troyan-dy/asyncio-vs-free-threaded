#!/bin/bash
# Same environment as the Dockerfile: CPython 3.14 (.venv-gil) and 3.14t (.venv-ft) with requirements.txt,
# plus the linter from requirements-dev.txt in .venv-gil (.venv-gil/bin/flake8 . --select=WPS).
set -euo pipefail

if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

cd "${CLAUDE_PROJECT_DIR:-$(dirname "$0")/../..}"

if ! command -v uv >/dev/null 2>&1; then
  python3 -m pip install --user -q uv
  export PATH="$HOME/.local/bin:$PATH"
fi

export UV_PYTHON_PREFERENCE=only-managed
uv python install 3.14 3.14t
for build in gil:3.14 ft:3.14t; do
  venv=".venv-${build%%:*}"
  [ -x "$venv/bin/python" ] || uv venv -q -p "${build#*:}" "$venv"
  uv pip install -q -p "$venv/bin/python" -r requirements.txt
done
uv pip install -q -p .venv-gil/bin/python -r requirements-dev.txt
.venv-ft/bin/python -c "import sys,fastapi,uvicorn,uvloop,httptools; assert not sys._is_gil_enabled(), 'GIL re-enabled'"
