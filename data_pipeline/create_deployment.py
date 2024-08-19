from prefect import flow
from prefect_github import GitHubCredentials

# github_credentials_block = GitHubCredentials.load("github")

# Source for the code to deploy (here, a GitHub repo)
SOURCE_REPO = "https://github.com/joshuaRiefman/sunbeam.git"

if __name__ == "__main__":
    flow.from_source(
        source=SOURCE_REPO,
        entrypoint="data_pipeline/data_pipeline.py:pipeline"
    ).deploy(
        name="test-deployment",
        work_pool_name="default-work-pool"
    )