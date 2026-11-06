import tomllib
from importlib import import_module

from stage import STAGE_REGISTRY_PATH
from stage.stage import Stage


# Every edition registers these, but the pipeline generator adds them itself; an event only chooses the rest
INGRESS_STAGES = ("Ingress", "Offline_Ingress")


def _load_registry() -> dict:
    with open(STAGE_REGISTRY_PATH, "rb") as f:
        return tomllib.load(f)


class StageLibrary:
    def __init__(self, pipeline_edition: str | None = None):
        self._raw_data = _load_registry()
        self._edition: dict = {}

        if pipeline_edition is not None:
            if pipeline_edition not in self._raw_data:
                raise ValueError(
                    f"Pipeline edition {pipeline_edition!r} not found. "
                    f"Must be one of {', '.join(self._raw_data)}"
                )
            self._edition = self._raw_data[pipeline_edition]

    @property
    def pipeline_editions(self) -> list[str]:
        return list(self._raw_data)

    @property
    def display_name(self) -> str:
        """ The edition's `name`, e.g. "Brightside 2024". """
        return self._edition.get("name", "")

    @property
    def stage_names(self) -> list[str]:
        """ The stages an event of this edition can run. """
        return [name for name, meta in self._edition.items() if isinstance(meta, dict) and name not in INGRESS_STAGES]

    def validate_pipeline_edition(self, pipeline_edition: str) -> None:
        if pipeline_edition not in self._raw_data:
            raise ValueError(
                f"Unknown pipeline edition {pipeline_edition!r}. Known editions: {self.pipeline_editions}"
            )

    def get_stage_by_name(self, stage_name: str) -> type[Stage]:
        """ Finds and returns the Stage subclass from its name

        :raises ValueError: Stage name not found in the edition's registry
        :raises TypeError: Found class from registry is not a stage subclass
        """
        meta = self._edition.get(stage_name)
        if not isinstance(meta, dict):
            raise ValueError(f"Stage {stage_name!r} not found.")

        # An entry like `[v4_0.Efficiency] from = "v3_0"` reuses that edition's stage of the same name, unchanged
        seen = []
        while "from" in meta:
            seen.append(meta["from"])
            meta = self._raw_data.get(meta["from"], {}).get(stage_name)
            if not isinstance(meta, dict):
                raise ValueError(f"Stage {stage_name!r} is taken from edition {seen[-1]!r}, which has no such stage.")
            if len(seen) > len(self._raw_data):
                raise ValueError(f"Stage {stage_name!r} is taken from editions in a loop: {seen}")

        cls = getattr(import_module(meta["module"]), meta["class"])

        if not issubclass(cls, Stage):
            raise TypeError(f"{cls} is not a Stage subclass.")

        return cls

    def get_stages_by_names(self, stage_names: list[str]) -> list[type[Stage]]:
        return [self.get_stage_by_name(name) for name in stage_names]
