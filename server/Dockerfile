FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

ENV UV_NO_CACHE=1

# git: the server resolves git refs (git ls-remote) for workers launched from a branch, tag, or commit
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates && rm -rf /var/lib/apt/lists/*

# Install dependencies first for Docker layer caching
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-install-project --extra server

COPY . .
RUN uv sync --locked --extra server

# --no-sync: the image is already synced, so starting needs no network
CMD ["uv", "run", "--no-sync", "uvicorn", "server.main:app", "--host", "0.0.0.0", "--port", "8000", "--timeout-graceful-shutdown", "5"]
