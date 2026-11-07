from fastapi import APIRouter

from server.schemas import PipelineEditionRead
from stage.stage_library import StageLibrary

router = APIRouter(prefix="/pipeline-editions", tags=["pipeline-editions"])


@router.get("", response_model=list[PipelineEditionRead])
def list_pipeline_editions():
    """ Every edition in stage/stage_registry.toml, with the stages an event of it can choose. """
    editions = []
    for edition in StageLibrary().pipeline_editions:
        library = StageLibrary(edition)
        editions.append(PipelineEditionRead(id=edition, name=library.display_name, stages=library.stage_names))
    return editions
