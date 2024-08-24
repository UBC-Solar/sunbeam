from prefect.blocks.core import Block


INFLUXDB_CREDENTIAL_BLOCK_NAME: str = "influxdb-credentials"


class InfluxCredentials(Block):
    influxdb_api_token: str
    influxdb_org: str
