FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

ARG PIPELINE_EDITION="v3_0"

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

# Install dependencies first for Docker layer caching
COPY pyproject.toml uv.lock ./

RUN uv sync --locked --extra executor --extra "${PIPELINE_EDITION}"

# Copy application code
COPY . .

# Run inside uv-managed environment
CMD ["uv", "run", "sunbeam.py"]