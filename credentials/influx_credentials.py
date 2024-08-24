from prefect.blocks.core import Block
from pydantic import SecretStr


class InfluxCredentials(Block):
    influxdb_api_token: SecretStr
    influxdb_org: SecretStr
