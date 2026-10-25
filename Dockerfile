# PROTOTYPE. One image with both interpreters (3.14 and 3.14t via uv) and both venvs.
FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates procps \
    && rm -rf /var/lib/apt/lists/*
COPY --from=ghcr.io/astral-sh/uv:0.13.5 /uv /uvx /bin/
ENV UV_PYTHON_INSTALL_DIR=/opt/python UV_LINK_MODE=copy UV_PYTHON_PREFERENCE=only-managed
RUN uv python install 3.14 3.14t
WORKDIR /app
# requirements.txt is generated from requirements.in: uv pip compile requirements.in -o requirements.txt --python-version 3.14 --universal
COPY requirements.txt /app/
RUN uv venv -q -p 3.14t /app/.venv-ft \
    && uv pip install -q -p /app/.venv-ft/bin/python -r requirements.txt \
    && uv venv -q -p 3.14 /app/.venv-gil \
    && uv pip install -q -p /app/.venv-gil/bin/python -r requirements.txt \
    && /app/.venv-ft/bin/python -c "import sys,fastapi,uvicorn,uvloop,httptools; assert not sys._is_gil_enabled(), 'GIL re-enabled'"
COPY *.py /app/
