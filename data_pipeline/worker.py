from influx_credentials import InfluxCredentials, INFLUXDB_CREDENTIAL_BLOCK_NAME
from dotenv import load_dotenv
import os
import logging

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()])
logger = logging.getLogger("PREFECT WORKER")


def prepare_environment():
    """
    Prepare the environment for the worker that is executing workflows
    """
    load_dotenv()

    InfluxCredentials(
        influxdb_api_token=os.getenv("INFLUX_TOKEN"),
        influxdb_org=os.getenv("INFLUX_ORG")
    ).save(INFLUXDB_CREDENTIAL_BLOCK_NAME, overwrite=True)
    logger.info("Saving credentials...")


if __name__ == "__main__":
    prepare_environment()
