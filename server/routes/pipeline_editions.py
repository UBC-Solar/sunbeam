from fastapi import APIRouter

from stage.stage_library import StageLibrary

router = APIRouter(prefix="/pipeline-editions", tags=["pipeline-editions"])


@router.get("")
def list_pipeline_editions() -> list[str]:
    return StageLibrary().pipeline_editions
