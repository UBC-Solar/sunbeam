FROM prefecthq/prefect:3-python3.12

RUN apt-get update && \
    apt-get install -y --no-install-recommends docker-cli && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY ./default-docker.json .

RUN pip install --no-cache-dir prefect==3.4.4 docker==7.1.0 prefect-docker==0.6.6

RUN which docker && docker --version

CMD ["sh"]

