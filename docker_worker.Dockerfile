FROM prefecthq/prefect:3-python3.12

USER root

# 3) Install the Docker CLI package
RUN apt-get update &&  \
    apt-get install -y --no-install-recommends docker.io && \
    apt-get clean &&  \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY "./default-docker.json" .

RUN pip install prefect==3.4.4 docker==7.1.0 prefect-docker==0.6.6

CMD ["sh"]
