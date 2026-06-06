from config.context import Context, ServiceType
from broker.server import Server
import tomllib
import config


if __name__ == "__main__":
    with open(config.CONTEXT_PATH, "rb") as f:
        config_dict = tomllib.load(f)
        Context.from_config(config_dict, ServiceType.Broker)

    broker_server = Server()