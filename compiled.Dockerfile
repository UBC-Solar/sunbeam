FROM python:3.12-slim

ARG BRANCH=main
ARG REPO_URL=https://github.com/UBC-Solar/sunbeam.git

# Install Git
RUN apt-get update && apt-get install -y git --no-install-recommends && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Setup project directory
WORKDIR /app

# Clone only the single branch.
# Force‐checkout the branch specified at build time, then immediately delete .git (to keep the image smaller).
RUN git clone --branch "$BRANCH" --single-branch "$REPO_URL" . && rm -rf sunbeam/.git

# Sync the project
ENV UV_SYSTEM_PYTHON=1
RUN uv sync --locked --compile-bytecode --no-editable --no-install-project --system


RUN pip list