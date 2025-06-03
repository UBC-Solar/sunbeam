FROM prefecthq/prefect:2.20-python3.12

USER root

# 3) Install the Docker CLI package
RUN apt-get update \
 && apt-get install -y --no-install-recommends docker.io \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

CMD ["sh"]
