# build_image.py

import os
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
    try:
        image, logs = client.images.build(
            path=path,
            tag=tag,
            buildargs=build_args or {},
            pull=False,       # do not pull base images from remote
            rm=True,          # remove intermediate containers
            forcerm=True,     # always remove intermediate containers
        )
        # You can optionally stream `logs` to see building progress
        for chunk in logs:
            if "stream" in chunk:
                sys.stdout.write(chunk["stream"])
        print(f"Successfully built image: {image.id[:12]}")
        return image
    except docker.errors.BuildError as e:
        print("Build failed:")
        for line in e.build_log:
            if "stream" in line:
                sys.stderr.write(line["stream"])
        raise

if __name__ == "__main__":
    """
    Example usage:
      python build_image.py <dockerfile_directory> my-branch-name
    """
    if len(sys.argv) < 3:
        print("Usage: python build_image.py <path_to_dockerfile_dir> <branch_name>")
        sys.exit(1)

    context_path = sys.argv[1]        # e.g. "./"
    branch_name   = sys.argv[2]        # e.g. "develop"

    # We pass a build-arg "BRANCH" into the Dockerfile (see next section).
    build_run_sunbeam_image(
        path=context_path,
        tag=f"run-sunbeam:{branch_name}",
        build_args={"BRANCH": branch_name},
    )
