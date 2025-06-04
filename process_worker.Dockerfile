FROM prefecthq/prefect:3-python3.12

# Prefect uses Git to acquire the pipeline from GitHub, so we need to install Git
RUN apt-get update && apt-get install -y git && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY "./default-process.json" .

COPY  ./pyproject.toml .

COPY ./uv.lock .

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

RUN uv sync --locked --no-install-project
