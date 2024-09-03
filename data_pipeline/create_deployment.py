from prefect import flow
from influx_credentials import InfluxCredentials, INFLUXDB_CREDENTIAL_BLOCK_NAME
# from data_pipeline.influx_credentials import InfluxCredentials
from dotenv import load_dotenv
import os


load_dotenv()


def prepare_env():
    InfluxCredentials(
        influxdb_api_token=os.getenv("INFLUX_TOKEN"),
        influxdb_org=os.getenv("INFLUX_ORG")
    ).save(INFLUXDB_CREDENTIAL_BLOCK_NAME)


if __name__ == "__main__":
    prepare_env()
