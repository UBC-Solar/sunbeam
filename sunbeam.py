from config.context import Context, ServiceType
from datetime import datetime, timezone
from sqlalchemy import create_engine
from pipeline import Executor
import tomllib
import config


class Sunbeam:
    def __init__(self):
        database_url = Context().sunbeam_db.build_url()
        self._engine = create_engine(database_url, echo=False)

    def __del__(self):
        self._engine.dispose()

    def run(self, event_name, reprocess: bool = False, debug: bool = False, debug_time: datetime = None):
        executor = Executor(event_name, self._engine, reprocess=reprocess, debug=debug, debug_time=debug_time)
        executor.run()

if __name__ == "__main__":
    with open(config.CONTEXT_PATH, "rb") as f:
        config_dict = tomllib.load(f)
        Context.from_config(config_dict, ServiceType.Client)

    sunbeam = Sunbeam()
    sunbeam.run("realtime", reprocess=True, debug=True, debug_time=datetime(2024, 7, 16, 14, 10, tzinfo=timezone.utc))
