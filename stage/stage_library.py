from importlib import import_module
from stage.stage import Stage
from stage import STAGE_REGISTRY_PATH
import tomllib
from typing import Optional


class StageLibrary:
    def __init__(self, pipeline_edition: Optional[str] = None):
        with open(STAGE_REGISTRY_PATH, "rb") as f:
            try:
                self._raw_data = tomllib.load(f)
                self._pipeline_editions = self._raw_data.keys()

                self._stage_registry = {}
                if pipeline_edition:
                    self.set_pipeline_edition(pipeline_edition)

            except KeyError:
                raise KeyError(f"{pipeline_edition!r} not found in {STAGE_REGISTRY_PATH}!")

    def set_pipeline_edition(self, pipeline_edition: str):
        if pipeline_edition not in self._pipeline_editions:
            raise ValueError(
                f"Pipeline edition {pipeline_edition!r} not found. Must be one of {', '.join(self._pipeline_editions)}")

        self._stage_registry: dict[str, dict[str, str]] = self._raw_data[pipeline_edition]

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

    @property
    def pipeline_editions(self) -> list[str]:
        return list(self._pipeline_editions)
