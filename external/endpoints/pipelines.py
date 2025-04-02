import prefect.client.schemas.responses
import logging
from prefect import flow
from prefect import exceptions as prefect_exceptions
from prefect_github import GitHubRepository
from prefect.client.orchestration import get_client
import subprocess
import pathlib
import asyncio
import re

BUILD_SCRIPT_PATH = pathlib.Path(__file__).parent.absolute() / "scripts" / "build_and_push.sh"
SOURCE_REPO = "https://github.com/UBC-Solar/sunbeam.git"

PIPELINE_NAME_PATTERN = r"pipeline-(.+)"


logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()])
logger = logging.getLogger()


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


def commission_pipeline(git_target):
    commissioned_pipelines = get_deployments()
    if git_target in commissioned_pipelines:
        return f"Pipeline {git_target} already commissioned!", 400

    repo_block = GitHubRepository(
        repository_url=SOURCE_REPO,
        reference=git_target
    )
    repo_block.save(name=f"source", overwrite=True)

    # We run a script that clones the repo, builds a Docker image with all the dependencies,
    # and then pushes the image to our Docker Hub organization, ubcsolarstrategy.
    try:
        result = subprocess.run(
            [f"./{BUILD_SCRIPT_PATH}", git_target],
            check=True,
            text=True,
            capture_output=True  # Captures both stdout and stderr
        )
        print(result)
    except subprocess.CalledProcessError as e:
        print(f"Command failed with exit code {e.returncode}")
        print(f"STDOUT: {e.stdout}")  # Output from the command (if any)
        print(f"STDERR: {e.stderr}")  # Error message

    flow.from_source(
        source=repo_block,
        entrypoint="pipeline/run.py:run_sunbeam"
    ).deploy(
        name=f"pipeline-{git_target}",
        work_pool_name="default-work-pool",
        image=f"ubcsolarstrategy/sunbeam:{git_target}",
        parameters={
            "git_target": git_target
        },
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
