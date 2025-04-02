FROM prefecthq/prefect:3.3.1-python3.12

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    lsb-release

# Install Docker CLI
RUN curl -fsSL https://get.docker.com | sh

# Prefect uses Git to acquire the pipeline from GitHub, so we need to install Git
RUN apt-get update && apt-get install -y git && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY ./pyproject.toml .
#
COPY ./poetry.lock .

COPY ./job_template.json .
#
#COPY --from=root ./config ./config
#
#COPY --from=root ./logs ./logs
#
#COPY --from=root ./data_source ./data_source
#
#COPY --from=root ./stage ./stage
#
#COPY . .

ENV POETRY_VERSION 1.8.3

RUN pip install poetry==$POETRY_VERSION

RUN poetry install