from datetime import datetime
import docker
import sys


def build_run_sunbeam_image(
    path: str,
    tag: str = "run-sunbeam:latest",
    build_args: dict[str,str] | None = None,
):
    """
    Builds a Docker image from `path` (where your Dockerfile lives),
    tags it with `tag`, and optionally passes build-args.
    """
    client = docker.from_env()

    print(f"Building image at {path} → tag '{tag}' …")

    image, logs = client.images.build(
        path="/",
        dockerfile="compiled.Dockerfile",
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


if __name__ == "__main__":
    context_path = "/Users/joshuariefman/Solar/sunbeam/compiled.Dockerfile"
    branch_name = "dev-make_docker_flows"

    # We pass a build-arg "BRANCH" into the Dockerfile (see next section).
    build_run_sunbeam_image(
        path=context_path,
        tag=f"run-sunbeam:{branch_name}",
        build_args={"BRANCH": branch_name, "CACHE_DATE": datetime.now().strftime("%Y-%m-%d")},
    )
