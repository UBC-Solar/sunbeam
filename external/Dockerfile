FROM python:3.12-slim

# Prefect uses Git to acquire the pipeline from GitHub, so we need to install Git
RUN apt-get update && apt-get install -y git && \
    apt-get clean && rm -rf /var/lib/apt/lists/* \

WORKDIR /app

COPY --from=root ./pyproject.toml .

COPY --from=root ./uv.lock .

COPY . .

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

RUN uv sync --locked --extra external