FROM prefecthq/prefect:3-python3.12

USER root

# 3) Install the Docker CLI package
RUN apt-get update &&  \
    apt-get install -y --no-install-recommends docker.io && \
    apt-get clean &&  \
    rm -rf /var/lib/apt/lists/* \

WORKDIR /app

RUN pip install prefect==3.4.4 docker prefect-docker

CMD ["sh"]
