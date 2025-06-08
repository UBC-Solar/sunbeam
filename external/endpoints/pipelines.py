import prefect.client.schemas.responses
from prefect.docker import DockerImage
from pipeline import run_sunbeam
import prefect.client.schemas.responses
import logging
from prefect import exceptions as prefect_exceptions
from prefect.client.orchestration import get_client
import docker
import datetime
import sys
import asyncio
import re


SOURCE_REPO = "https://github.com/UBC-Solar/sunbeam.git"
PIPELINE_NAME_PATTERN = r"pipeline-(.+)"


logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()])
logger = logging.getLogger()


def build_run_sunbeam_image(
    tag: str = "run-sunbeam:latest",
    dockerfile: str = "compiled.Dockerfile",
    build_args: dict[str,str] | None = None,
):
    """
    Builds a Docker image from `path` (where your Dockerfile lives),
    tags it with `tag`, and optionally passes build-args.
    """
    client = docker.from_env()

    image, logs = client.images.build(
        path="/build/",
        dockerfile=f"{dockerfile}",
        tag=tag,
        buildargs=build_args or {},
        pull=False,       # do not pull base images from remote
        rm=True,          # remove intermediate containers
        forcerm=True,     # always remove intermediate containers
    )

    for chunk in logs:
        if "stream" in chunk:
            sys.stdout.write(chunk["stream"])

    print(f"Successfully built image: {image.id[:12]}")
    return image


def decommission_pipeline(collection, git_target):
    commissioned_pipelines = get_deployments()
    if git_target not in commissioned_pipelines:
        return f"Pipeline {git_target} is not commissioned!", 400

    collection.delete_many({"origin": git_target})

    async def delete_deployment_by_name(deployment_name):
        async with get_client() as prefect_client:
            try:
                # The name syntax needs to be updated to the same as when deployments are created
                deployment = await prefect_client.read_deployment_by_name(f"run-sunbeam/pipeline-{deployment_name}")
                assert isinstance(deployment, prefect.client.schemas.responses.DeploymentResponse)

                print(f"Decomissioning {deployment_name}")

                await prefect_client.delete_deployment(deployment.id)

            except (AttributeError, AssertionError, prefect_exceptions.ObjectNotFound):
                logger.error(f"Failed to delete deployment: {deployment_name}")

    asyncio.run(delete_deployment_by_name(git_target))

    return f"Decommissioned {git_target}!", 200


def commission_pipeline(git_target, build_local=False):
    commissioned_pipelines = get_deployments()
    if git_target in commissioned_pipelines:
        return f"Pipeline {git_target} already commissioned!", 400

    dockerfile_name = "local.Dockerfile" if build_local else "compiled.Dockerfile"

    build_run_sunbeam_image(
        dockerfile=dockerfile_name,
        tag=f"run-sunbeam:{git_target}",
        build_args={"BRANCH": git_target, "CACHE_DATE": datetime.datetime.now().strftime("%Y-%m-%d")}
    )

    run_sunbeam.deploy(
        name=f"pipeline-{git_target}",
        work_pool_name="docker-work-pool",
        image=DockerImage(
            name="run-sunbeam",
            tag=f"{git_target}",
            dockerfile="compiled.Dockerfile"
        ),
        parameters={
            "git_target": git_target,
            "stages_to_skip": [],
            "ingress_to_skip": []
        },
        push=False,
        build=False
    )

    async def run_deployment_by_name(deployment_name):
        async with get_client() as prefect_client:
            try:
                deployment = await prefect_client.read_deployment_by_name(f"run-sunbeam/pipeline-{deployment_name}")
                assert isinstance(deployment, prefect.client.schemas.responses.DeploymentResponse)

                await prefect_client.create_flow_run_from_deployment(deployment.id)

            except AssertionError:
                logger.error(f"Failed to run deployment {deployment_name}")

    asyncio.run(run_deployment_by_name(git_target))

    return f"Commissioned {git_target}", 200


def get_deployments():
    async def read_deployments():
        async with get_client() as prefect_client:
            deployments = await prefect_client.read_deployments()

            assert isinstance(deployments, list)

            deployment_names = []
            for deployment in deployments:
                pipeline_name_match = re.search(PIPELINE_NAME_PATTERN, deployment.name)

                if pipeline_name_match:
                    deployment_names.append(pipeline_name_match.group(1))

            return deployment_names

    return asyncio.run(read_deployments())


def list_commissioned_pipelines():
    return get_deployments()
