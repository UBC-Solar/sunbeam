from influx_credentials import InfluxCredentials, INFLUXDB_CREDENTIAL_BLOCK_NAME
from dotenv import load_dotenv
import os


def prepare_environment():
    """
    Prepare the environment for the worker that is executing workflows
    """
    load_dotenv()

    InfluxCredentials(
        influxdb_api_token=os.getenv("INFLUX_TOKEN"),
        influxdb_org=os.getenv("INFLUX_ORG")
    ).save(INFLUXDB_CREDENTIAL_BLOCK_NAME)
    print("Saving credentials...")


if __name__ == "__main__":
    prepare_environment()
