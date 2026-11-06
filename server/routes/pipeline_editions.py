from fastapi import APIRouter

from stage.stage_library import StageLibrary

router = APIRouter(prefix="/pipeline-editions", tags=["pipeline-editions"])


@router.get("")
def list_pipeline_editions() -> list[dict]:
    """ Every edition in stage_registry.toml, with the stages an event of that edition can run. """
    editions = []
    for edition in StageLibrary().pipeline_editions:
        library = StageLibrary(edition)
        editions.append({"id": edition, "name": library.display_name, "stages": library.stage_names})
    return editions
