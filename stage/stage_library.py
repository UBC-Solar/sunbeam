from stage.node import Node


class StageLibrary:
    @staticmethod
    def get_stage_by_name(stage_name: str) -> Node:
        stages = Node.__subclasses__()
        for stage in stages:
            if stage_name == stage.node_name:
                return stage()

        raise Exception(f"Stage {stage_name} not found.")

    @staticmethod
    def get_stages_by_names(stage_names: list[str]) -> list[Node]:
        return [StageLibrary.get_stage_by_name(stage_name) for stage_name in stage_names]
    