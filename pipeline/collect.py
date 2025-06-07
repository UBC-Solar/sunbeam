import pathlib

from data_tools import Event, FileType
import toml as tomllib
from typing import List, Union
import traceback
from logs import SunbeamLogger
from data_tools.query.influxdb_query import TimeSeriesTarget
from config import config_directory


logger = SunbeamLogger("sunbeam")


def collect_targets(ingress_config: dict) -> List[TimeSeriesTarget]:
    targets = []
    seen_names = set()

    for target in ingress_config["target"]:
        try:
            target_type = FileType(target["type"])
        except ValueError:
            # File type not known by data tools, keep it as a string
            target_type = target["type"]

        match target_type:
            case FileType.TimeSeries:
                if not target["name"] in seen_names:
                    try:
                        targets.append(TimeSeriesTarget(**target))

                    except KeyError:
                        logger.error(f"Missing key in target! \n {traceback.format_exc()}")

                else:
                    raise ValueError(f"Target names must be unique! {target['name']} "
                                     f"is already the name of another target.")

            case "DataFrame":
                if not target["name"] in seen_names:
                    # try:
                    #     targets.append(DataFrameTarget(**target))
                    #
                    # except KeyError:
                    #     logger.error(f"Missing key in target! \n {traceback.format_exc()}")

                else:
                    raise ValueError(f"Target names must be unique! {target['name']} "
                                     f"is already the name of another target.")

            case _:
                raise NotImplementedError(f"Ingress of {target_type} type not implemented!")

    assert len(targets) > 0, "Unable to identify any targets!"

    return targets


def collect_events(events_description_filename: Union[str, pathlib.Path]) -> List[Event]:
    events_filepath = config_directory / events_description_filename

    with open(events_filepath) as events_description_file:
        file = tomllib.load(events_description_file)
        events_descriptions = file["event"]

    events = list([Event.from_dict(event) for event in events_descriptions])

    assert len(events) > 0, f"No events were collected in {events_filepath}! At least one event must be declared."

    return events


def collect_config_file(config_file: Union[str, pathlib.Path]) -> dict:
    config_path = config_directory / config_file
    logger.info(f"Trying to find config at {config_path}...")

    with open(config_directory / config_path) as config_file:
        config = tomllib.load(config_file)

    logger.info(f"Acquired config from {config_path}:\n")
    logger.info(tomllib.dumps(config) + "\n")

    return config
