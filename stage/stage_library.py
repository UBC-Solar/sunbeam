from importlib import import_module
from stage.stage import Stage
from stage import STAGE_REGISTRY_PATH
import tomllib


class StageLibrary:
    def __init__(self, pipeline_edition: str):
        with open(STAGE_REGISTRY_PATH, "rb") as f:
            try:
                self._stage_registry: dict[str, dict[str, str]] = tomllib.load(f)[pipeline_edition]
            except KeyError:
                raise KeyError(f"{pipeline_edition!r} not found in {STAGE_REGISTRY_PATH}!")

    def get_stage_by_name(self, stage_name: str) -> type[Stage]:
        if stage_name not in self._stage_registry:
            raise ValueError(f"Stage {stage_name!r} not found.")

        meta = self._stage_registry[stage_name]

        module = import_module(meta["module"])
        cls = getattr(module, meta["class"])

        if not issubclass(cls, Stage):
            raise TypeError(f"{cls} is not a Stage subclass.")

        return cls

    def get_stages_by_names(self, stage_names: list[str]) -> list[type[Stage]]:
        return [self.get_stage_by_name(name) for name in stage_names]