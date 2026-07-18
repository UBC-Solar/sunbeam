from fastapi import APIRouter

from server.services.pipeline_service import PipelineService


router = APIRouter(prefix="/pipeline-editions", tags=["pipeline-editions"])


@router.get("")
def list_pipeline_editions() -> list[str]:
    return PipelineService().list_pipeline_editions()