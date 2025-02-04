FROM prefecthq/prefect:2.20-python3.12

# Prefect uses Git to acquire the pipeline from GitHub, so we need to install Git
RUN apt-get update && apt-get install -y git && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=root ./pyproject.toml .

COPY --from=root ./poetry.lock .

COPY --from=root ./config .

COPY --from=root ./logs .

COPY --from=root ./data_source .

COPY --from=root ./stage .

COPY . .

ENV POETRY_VERSION 1.8.3

RUN pip install poetry==$POETRY_VERSION

RUN poetry install