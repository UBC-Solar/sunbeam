FROM python:3.12-slim

ARG BRANCH=dev-make_docker_flows
ARG REPO_URL=https://github.com/UBC-Solar/sunbeam.git

# Install Git
RUN apt-get update && apt-get install -y git --no-install-recommends && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Setup project directory
WORKDIR /app

# Docker would cache the GitHub repository clone otherwise, so we need to artifically invalidate the cache
# by presenting a layer before it that will always change, and so invalidate it.
ARG CACHE_DATE

# Clone only the single branch.
# Force‐checkout the branch specified at build time, then immediately delete .git (to keep the image smaller).
RUN git clone --branch "$BRANCH" --single-branch "$REPO_URL" . && rm -rf sunbeam/.git

# Set the project environment to the system Python.
ENV UV_PROJECT_ENVIRONMENT=/usr/local

# Sync the project.
RUN uv sync --locked --compile-bytecode --no-editable --no-install-project