from flask import Flask
import pymongo
from typing import List
import logging
from prefect import flow
from prefect_github import GitHubRepository

SOURCE_REPO = "https://github.com/joshuaRiefman/sunbeam.git"


logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()])
logger = logging.getLogger()


app = Flask(__name__)


def init_db():
    metadata_collection.insert_one({
        "type": "status"
    })

    metadata_collection.insert_one({
        "type": "commissioned_pipelines",
        "data": []
    })


client = pymongo.MongoClient("mongodb://mongodb:27017/")

db = client.sunbeam_db

metadata_collection: pymongo.collection.Collection = db.metadata
time_series_collection: pymongo.collection.Collection = db.time_series_data
db_status = metadata_collection.find_one({"type": "status"})


if db_status is None:
    logger.info("MongoDB is not initialized. Initializing...")
    init_db()

logger.info("MongoDB is initialized!")

commissioned_pipelines_query = {
    "type": "commissioned_pipelines"
}


@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"


@app.route("/list_files")
def list_files():
    res: List[str] = []

    files = time_series_collection.find()
    for file in files:
        res.append(f"{file['code_hash']}/{file['event']}/{file['field']}")

    return res


@app.route("/decommission_pipeline/<code_hash>")
def decommission_pipeline(code_hash):
    commissioned_pipelines = metadata_collection.find_one(commissioned_pipelines_query)['data']
    if code_hash not in commissioned_pipelines:
        return f"Pipeline {code_hash} is not commissioned!"

    time_series_collection.delete_many({"code_hash": code_hash})

    update = {
        "$pull": {
            "data": code_hash
        }
    }

    metadata_collection.update_one(commissioned_pipelines_query, update)

    return f"Decommissioned {code_hash}!"


@app.route("/commission_pipeline/<code_hash>")
def commission_pipeline(code_hash):
    commissioned_pipelines = metadata_collection.find_one(commissioned_pipelines_query)['data']
    if code_hash in commissioned_pipelines:
        return f"Pipeline {code_hash} already commissioned!"

    repo_block = GitHubRepository(
        repository_url=SOURCE_REPO,
        reference=code_hash
    )
    repo_block.save(name=f"source", overwrite=True)

    flow.from_source(
        source=repo_block,
        entrypoint="data_pipeline/pipeline.py:pipeline"
    ).deploy(
        name=f"pipeline-{code_hash}",
        work_pool_name="default-work-pool",
        parameters={
            "git_tag": code_hash
        }
    )

    update = {
        "$push": {
            "data": code_hash
        }
    }

    metadata_collection.update_one(commissioned_pipelines_query, update)

    return f"Commissioned {code_hash}"


@app.route("/list_commissioned_pipelines")
def list_commissioned_pipelines():
    commissioned_pipelines = metadata_collection.find_one(commissioned_pipelines_query)
    return commissioned_pipelines['data']


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8080)
