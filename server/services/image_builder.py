"""
Builds worker images: one per pipeline edition from the server's own copy of
the code (at startup), or on demand from a git branch, tag, or commit, which
pipeline/Dockerfile.git clones itself.
"""
import io
import logging
import pathlib
import re
import subprocess
from collections.abc import Callable

from docker.errors import BuildError, ImageNotFound

logger = logging.getLogger("sunbeam.server")

PROJECT_ROOT = pathlib.Path(__file__).parent.parent.parent.absolute()
GIT_DOCKERFILE = PROJECT_ROOT / "pipeline" / "Dockerfile.git"
DEFAULT_GIT_REMOTE = "https://github.com/UBC-Solar/sunbeam.git"

_FULL_SHA = re.compile(r"[0-9a-f]{40}")
_SHORT_SHA = re.compile(r"[0-9a-f]{7,39}")

# How much of a failed build's output to keep on the worker run
BUILD_LOG_TAIL = 20


class ImageBuildFailed(RuntimeError):
    pass


def _git_ls_remote(remote: str, ref: str) -> str:
    completed = subprocess.run(
        ["git", "ls-remote", remote, ref],
        capture_output=True, text=True, timeout=30, check=False,
    )
    if completed.returncode != 0:
        raise ValueError(f"Could not list refs on {remote}: {completed.stderr.strip()}")
    return completed.stdout


def _build_log_text(build_log) -> str:
    lines: list[str] = []
    for chunk in build_log or []:
        if isinstance(chunk, dict):
            text = chunk.get("stream") or chunk.get("error") or ""
            lines.extend(line for line in text.splitlines() if line.strip())
    return "\n".join(lines[-BUILD_LOG_TAIL:])


class WorkerImageBuilder:
    def __init__(
        self,
        docker_client,
        *,
        remote: str = DEFAULT_GIT_REMOTE,
        ls_remote: Callable[[str, str], str] = _git_ls_remote,
    ) -> None:
        self._docker = docker_client
        self._remote = remote
        self._ls_remote = ls_remote

    @staticmethod
    def local_tag(pipeline_edition: str) -> str:
        return f"sunbeam-worker:{pipeline_edition}"

    @staticmethod
    def git_tag(pipeline_edition: str, sha: str) -> str:
        return f"sunbeam-worker:{pipeline_edition}-{sha[:12]}"

    def resolve_ref(self, ref: str) -> str:
        """
        The commit a branch, tag, or full SHA names on the remote. Resolving it here (not in the Dockerfile) is
        what lets a moved branch rebuild while a commit that's already been built reuses its image.
        """
        ref = ref.strip()
        if not ref:
            raise ValueError("Git ref is empty")
        if _FULL_SHA.fullmatch(ref):
            return ref

        listed: dict[str, str] = {}
        for line in self._ls_remote(self._remote, ref).splitlines():
            sha, _, name = line.partition("\t")
            listed[name.strip()] = sha.strip()

        # git's own precedence for an ambiguous name; ^{} is the commit an annotated tag points at
        for name in (ref, f"refs/tags/{ref}^{{}}", f"refs/tags/{ref}", f"refs/heads/{ref}"):
            if name in listed:
                return listed[name]

        if _SHORT_SHA.fullmatch(ref):
            raise ValueError(f"{ref!r} looks like a short commit SHA; give the full 40-character SHA instead")
        raise ValueError(f"No branch or tag named {ref!r} on {self._remote}")

    def has_image(self, tag: str) -> bool:
        try:
            self._docker.images.get(tag)
            return True
        except ImageNotFound:
            return False

    def build_local(self, pipeline_edition: str) -> str:
        tag = self.local_tag(pipeline_edition)
        logger.info("Building worker image: %s", tag)

        self._docker.images.build(
            path=str(PROJECT_ROOT),
            dockerfile="pipeline/Dockerfile",
            tag=tag,
            rm=True,
            buildargs={"PIPELINE_EDITION": pipeline_edition},
        )

        logger.info("Built worker image: %s", tag)
        return tag

    def build_git(self, pipeline_edition: str, sha: str) -> str:
        """ Build (or reuse) the image for a resolved commit. The build context is only the Dockerfile. """
        tag = self.git_tag(pipeline_edition, sha)
        if self.has_image(tag):
            logger.info("Reusing worker image %s", tag)
            return tag

        logger.info("Building worker image %s from %s@%s", tag, self._remote, sha)
        try:
            self._docker.images.build(
                fileobj=io.BytesIO(GIT_DOCKERFILE.read_bytes()),
                tag=tag,
                rm=True,
                buildargs={
                    "PIPELINE_EDITION": pipeline_edition,
                    "GIT_REMOTE": self._remote,
                    "GIT_REF": sha,
                },
            )
        except BuildError as exc:
            raise ImageBuildFailed(f"{exc.msg}\n{_build_log_text(exc.build_log)}".strip()) from exc

        logger.info("Built worker image: %s", tag)
        return tag
