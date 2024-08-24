from prefect import flow
from influx_credentials import InfluxCredentials, INFLUXDB_CREDENTIAL_BLOCK_NAME
# from data_pipeline.influx_credentials import InfluxCredentials
from dotenv import load_dotenv
import os

load_dotenv()

InfluxCredentials(
    influxdb_api_token=os.getenv("INFLUXDB_TOKEN"),
    influxdb_org=os.getenv("INFLUXDB_ORG")
).save(INFLUXDB_CREDENTIAL_BLOCK_NAME)

# Source for the code to deploy (here, a GitHub repo)
SOURCE_REPO = "https://github.com/joshuaRiefman/sunbeam.git"

if __name__ == "__main__":
    flow.from_source(
        source=SOURCE_REPO,
        entrypoint="data_pipeline/data_pipeline.py:pipeline"
    ).deploy(
        name="test-deployment",
        work_pool_name="default-work-pool"
    )
