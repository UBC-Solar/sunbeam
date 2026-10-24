from fastapi import APIRouter, Depends

from server.routes.workers import get_worker_service
from server.schemas import ConfigurationProfilesRead
from server.services.worker_service import WorkerService


router = APIRouter(prefix="/configuration-profiles", tags=["configuration-profiles"])


@router.get("", response_model=ConfigurationProfilesRead)
def list_configuration_profiles(service: WorkerService = Depends(get_worker_service)):
    """ The context.toml profiles a launched worker can use, and the one it gets by default. """
    return ConfigurationProfilesRead(
        profiles=service.configuration_profiles,
        default=service.default_configuration_profile,
    )
