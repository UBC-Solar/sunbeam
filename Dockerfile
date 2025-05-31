FROM prefecthq/prefect:2.20-python3.12

# Prefect uses Git to acquire the pipeline from GitHub, so we need to install Git
RUN apt-get update && apt-get install -y git && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY  ./pyproject.toml .

COPY ./poetry.lock .

ENV POETRY_VERSION 2.0.1

RUN pip install poetry==$POETRY_VERSION

RUN poetry install