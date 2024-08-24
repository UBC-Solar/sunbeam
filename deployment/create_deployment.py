import os

from prefect import flow


CredentialsBlock(
    influxdb_api_token=os.getenv("INFLUXDB_TOKEN"),
    influxdb_org=os.getenv("INFLUXDB_ORG")
).save("influxdb_credentials")

SOURCE_REPO = "https://github.com/joshuaRiefman/sunbeam.git"

if __name__ == "__main__":
    flow.from_source(
        source=SOURCE_REPO,
        entrypoint="data_pipeline/data_pipeline.py:pipeline"
    ).deploy(
        name="test-deployment",
        work_pool_name="default-work-pool"
    )