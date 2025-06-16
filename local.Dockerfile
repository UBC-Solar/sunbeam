FROM python:3.12-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Setup project directory
WORKDIR /app

COPY ./pyproject.toml .
COPY ./uv.lock .

# Set the project environment to the system Python.
ENV UV_PROJECT_ENVIRONMENT=/usr/local

# Sync the project
RUN uv sync --locked --compile-bytecode --no-editable --no-install-project

# Docker would cache the local repository otherwise, so we need to artifically invalidate the cache
# by presenting a layer before it that will always change, and so invalidate it.
ARG CACHE_DATE

# Copy in the rest of the project
COPY ./pipeline/ ./pipeline/
COPY ./logs/ ./logs/
COPY ./data_source/ ./data_source
COPY ./config/ ./config/
COPY ./stage/ ./stage/
