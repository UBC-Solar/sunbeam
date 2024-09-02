FROM python:3.11-slim

# We need Git as the data pipeline source is a GitHub repo
RUN apt-get update && apt-get install -y git && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . .

ENV POETRY_VERSION 1.8.3

RUN pip install poetry==$POETRY_VERSION

RUN poetry add prefect

RUN poetry install
