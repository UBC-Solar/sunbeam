FROM python:3.12-slim

# Prefect uses Git to acquire the pipeline from GitHub, so we need to install Git
RUN apt-get update &&  \
    apt-get install -y git && \
    apt-get install -y --no-install-recommends docker.io && \
    apt-get clean &&  \
    rm -rf /var/lib/apt/lists/* \

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

COPY  ./pyproject.toml .

COPY  ./uv.lock .

RUN uv sync --locked --extra external --no-install-project

COPY . .

RUN uv sync --locked --extra external