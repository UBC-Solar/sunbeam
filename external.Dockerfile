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

COPY ./external ./external
COPY ./pipeline ./pipeline

RUN mkdir build

COPY ./logs ./build/logs
COPY ./pipeline ./build/pipeline
COPY ./stage ./build/stage
COPY ./config ./build/config
COPY ./data_source ./build/data_source
COPY ./compiled.Dockerfile ./build/compiled.Dockerfile
COPY ./local.Dockerfile ./build/local.Dockerfile
COPY  ./pyproject.toml ./build/pyproject.toml
COPY  ./uv.lock ./build/uv.lock

#RUN ln -s ./logs ./build/logs
#RUN ln -s ./pipeline ./build/pipeline
#RUN ln -s ./stage ./build/stage
#RUN ln -s ./config ./build/config
#RUN ln -s ./data_source ./build/data_source
#RUN ln -s ./pyproject.toml ./build/pyproject.toml
#RUN ln -s ./uv.lock ./build/uv.lock
