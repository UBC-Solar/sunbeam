from data_tools import Event
from typing import List
import networkx as nx
from data_tools.query.influxdb_query import TimeSeriesTarget
from stage import stage_registry
from config import SunbeamConfig, DataSourceConfig, DataSourceConfigFactory
from pipeline.collect import collect_targets, collect_events, collect_config_file


def build_config() -> tuple[SunbeamConfig, DataSourceConfig, DataSourceConfig, List[TimeSeriesTarget], List[Event]]:
    config_file: dict = collect_config_file("sunbeam.toml")
    sunbeam_config: SunbeamConfig = SunbeamConfig(**config_file["config"])

    stage_data_source_type = config_file["stage_data_source"]["data_source_type"]
    ingress_data_source_type = config_file["ingress_data_source"]["data_source_type"]

    data_source_config: DataSourceConfig = DataSourceConfigFactory.build(stage_data_source_type, config_file["stage_data_source"])
    ingress_config: DataSourceConfig = DataSourceConfigFactory.build(ingress_data_source_type, config_file["ingress_data_source"])

    targets_file: dict = collect_config_file(sunbeam_config.ingress_description_file)
    targets: List[TimeSeriesTarget] = collect_targets(targets_file)

    events: List[Event] = collect_events(sunbeam_config.events_description_file)
    print(events)
    return sunbeam_config, data_source_config, ingress_config, targets, events


def _add_dependencies(dependency_graph: nx.Graph, stage_id: str):
    dependency_graph.add_node(stage_id)

    stage_cls = stage_registry.get_stage(stage_id)
    for dep_id in stage_cls.dependencies():
        _add_dependencies(dependency_graph, dep_id)


def build_stage_graph(stages_to_run: List[str]) -> List[str]:
    # Build a dependency graph
    dependency_graph = nx.DiGraph()

    # Add nodes and edges based on dependencies
    for stage_id in stages_to_run:
        _add_dependencies(dependency_graph, stage_id)

    # Check for cycles and determine execution order
    required_stages = list(nx.topological_sort(dependency_graph))
    required_stages.reverse()  # The topological sort will be in the inverse order, so we need to just reverse it

    return required_stages
