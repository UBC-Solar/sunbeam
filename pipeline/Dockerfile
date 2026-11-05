FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

# Built without BuildKit (docker-py), so uv's cache can't be a cache mount; it would only bloat the layers
ENV UV_NO_CACHE=1

RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates && rm -rf /var/lib/apt/lists/*

# Install dependencies first for Docker layer caching
COPY pyproject.toml uv.lock ./

# Dependencies every edition shares, in a layer they all reuse; the edition's extra adds only what it needs on top
RUN uv sync --locked --no-install-project --extra executor

ARG PIPELINE_EDITION="v3_0"

RUN uv sync --locked --no-install-project --extra executor --extra "${PIPELINE_EDITION}"

# Copy application code
COPY . .

RUN uv sync --locked --extra executor --extra "${PIPELINE_EDITION}"

# Run inside uv-managed environment. --no-sync: the image is already synced, so starting needs no network.
CMD ["uv", "run", "--no-sync", "sunbeam.py"]
