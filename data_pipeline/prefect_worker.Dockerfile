FROM prefecthq/prefect:2.20-python3.11

RUN apt-get update && apt-get install -y git && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY poetry.lock .

COPY pyproject.toml .

COPY .env .

ENV POETRY_VERSION 1.8.3

RUN ls

RUN pip install poetry==$POETRY_VERSION

RUN poetry add prefect

RUN poetry install