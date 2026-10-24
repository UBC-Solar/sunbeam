from stage.stage_library import StageLibrary


class PipelineService:
    def __init__(self) -> None:
        self._library = StageLibrary()

    def list_pipeline_editions(self) -> list[str]:
        return list(self._library.pipeline_editions)

    def image_tag_for(self, pipeline_edition: str) -> str:
        return f"sunbeam-worker:{pipeline_edition}"

    def validate_pipeline_edition(self, pipeline_edition: str) -> None:
        editions = self.list_pipeline_editions()
        if pipeline_edition not in editions:
            raise ValueError(
                f"Unknown pipeline edition {pipeline_edition!r}. "
                f"Known editions: {editions}"
            )