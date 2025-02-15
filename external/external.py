import io

import prefect.client.schemas.responses
from flask import Flask, render_template, request, send_file
import pymongo
from typing import List
import logging
from prefect import flow
from prefect import exceptions as prefect_exceptions
from prefect_github import GitHubRepository
from prefect.client.orchestration import get_client
import asyncio
import tempfile
from bokeh.plotting import figure, output_file, save
from bokeh.models import ColumnDataSource, DatetimeTickFormatter
from data_tools.collections import TimeSeries
from data_tools.schema import File, CanonicalPath
import dill
import re


SOURCE_REPO = "https://github.com/UBC-Solar/sunbeam.git"


PIPELINE_NAME_PATTERN = r"pipeline-(.+)"


logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()])
logger = logging.getLogger()

app = Flask(__name__)


client = pymongo.MongoClient("mongodb://mongodb:27017/")

db = client.sunbeam_db

time_series_collection = db.time_series_data

logger.info("MongoDB is initialized!")


@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"


@app.route("/list_files")
def list_files():
    res: List[str] = []

    files = time_series_collection.find({}, {"origin": 1, "event": 1, "source": 1, "name": 1})
    for file in files:
        res.append(f"{file['origin']}/{file['event']}/{file['source']}/{file['name']}")

    return res


@app.route("/pipeline/decommission_pipeline", methods=['POST', 'GET'])
def decommission_pipeline():
    if request.method == 'POST':
        git_target = request.form.get('git_target')

        commissioned_pipelines = get_deployments()
        if git_target not in commissioned_pipelines:
            return f"Pipeline {git_target} is not commissioned!"

        time_series_collection.delete_many({"origin": git_target})

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

        return f"Decommissioned {git_target}!"

    else:
        return render_template("decommission.html")


@app.route("/pipeline", methods=['GET'])
def pipeline():
    return render_template("pipelines.html")


@app.route("/pipeline/commission_pipeline", methods=['GET', 'POST'])
def commission_pipeline():
    if request.method == 'POST':
        git_target = request.form.get('git_target')

        commissioned_pipelines = get_deployments()
        if git_target in commissioned_pipelines:
            return f"Pipeline {git_target} already commissioned!"

        repo_block = GitHubRepository(
            repository_url=SOURCE_REPO,
            reference=git_target
        )
        repo_block.save(name=f"source", overwrite=True)

        flow.from_source(
            source=repo_block,
            entrypoint="pipeline/run.py:run_sunbeam"
        ).deploy(
            name=f"pipeline-{git_target}",
            work_pool_name="default-work-pool",
            parameters={
                "git_target": git_target
            }
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

        return f"Commissioned {git_target}"

    else:
        return render_template('commission.html')


@app.route('/files', defaults={'path': ''})
@app.route('/files/<path:path>')
def show_hierarchy(path):
    path_parts = path.split('/') if path else []

    # User is querying the available commissioned pipelines
    if len(path_parts) == 0:
        # Top-level directories
        results = time_series_collection.distinct("origin")
        return render_template("list.html", items=results, path=path, title="Pipelines")

    # User is querying the events that a pipeline processed
    elif len(path_parts) == 1:
        results = time_series_collection.distinct("event", {"origin": path_parts[0]})
        return render_template("list.html", items=results, path=path, title=f"Events of {path_parts[0]}")

    # User is querying the stages that were processed for an event and pipeline
    elif len(path_parts) == 2:
        results = time_series_collection.distinct("source", {
            "origin": path_parts[0],
            "event": path_parts[1]
        })
        return render_template("list.html", items=results, path=path, title=f"Stages of {path_parts[0]}/{path_parts[1]}")

    # User is querying the files produced by a stage for an event and pipeline
    elif len(path_parts) == 3:
        results = time_series_collection.distinct("name", {
            "origin": path_parts[0],
            "event": path_parts[1],
            "source": path_parts[2],
        })
        print(results)
        return render_template("list.html", items=results, path=path, title=f"Files of {path_parts[0]}/{path_parts[1]}/{path_parts[2]}")

    # User is querying a specific file
    elif len(path_parts) == 4:
        results = time_series_collection.find_one({
            "origin": path_parts[0],
            "event": path_parts[1],
            "source": path_parts[2],
            "name": path_parts[3],
        })

        if "file_type" not in request.args.keys():
            return render_template('access.html', file_types=["bin", "plot"], file_name=path_parts[3])

        else:
            file_name = path_parts[3]

            match file_type := request.args.get("file_type"):
                case "bin":
                    file = File(
                        canonical_path=CanonicalPath(
                            origin=path_parts[0],
                            event=path_parts[1],
                            source=path_parts[2],
                            name=path_parts[3],
                        ),
                        data=dill.loads(results["data"]),
                        metadata=results["metadata"],
                        file_type=results["filetype"],
                        description=results["description"]
                    )

                    file_stream = io.BytesIO(dill.dumps(file, protocol=dill.HIGHEST_PROTOCOL))
                    file_stream.seek(0)

                    return send_file(file_stream, as_attachment=True, download_name=f"{file_name}.{file_type}")

                case "plot":
                    data: TimeSeries = dill.loads(results["data"])

                    return _create_bokeh_plot(data, path_parts[3])

                case _:
                    return "Invalid File Type!", 404

    return "Invalid Path!", 404


def _create_bokeh_plot(data: TimeSeries, title: str) -> str:
    """
    Create an interactive Bokeh plot as raw HTML from ``data``.

    :param TimeSeries data: the time-series data to be plotted
    :param str title: the title of the plot
    :return: HTML as a string of the interactive Bokeh plot
    """

    # Bokeh does not support just dumping the HTML as a string. So, we will force
    # it to by telling it to write to a fake (temporary) file, which we can read
    # to extract the HTML of the plot as a string.
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        # Specify the temporary file as the output for Bokeh
        output_file(temp_file.name)

        source = ColumnDataSource(data=dict(dates=data.datetime_x_axis, values=data))

        # Create a figure with a datetime x-axis
        p = figure(title=title, x_axis_type='datetime', x_axis_label='Date',
                   y_axis_label=data.units)

        p.xaxis.formatter = DatetimeTickFormatter(
            hours="%d %Hh",
            days="%d %Hh",
            months="%d %Hh",
            years="%d %Hh"
        )

        # Add a line renderer
        p.line('dates', 'values', source=source, legend_label="Values", line_width=2)

        save(p)

        # Read the HTML content from the temporary file
        with open(temp_file.name, 'r') as f:
            html_content = f.read()

            return html_content


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


@app.route("/list_commissioned_pipelines")
def list_commissioned_pipelines():
    return get_deployments()


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8080)
