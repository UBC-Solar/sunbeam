import io

from flask import render_template, request, send_file
from typing import List
import logging
import tempfile
from bokeh.plotting import figure, output_file, save
from bokeh.models import ColumnDataSource, DatetimeTickFormatter
from data_tools.collections import TimeSeries
from data_tools.schema import File, CanonicalPath
import dill


logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()])
logger = logging.getLogger()


def list_files(collection):
    res: List[str] = []

    files = collection.find({}, {"origin": 1, "event": 1, "source": 1, "name": 1})
    for file in files:
        res.append(f"{file['origin']}/{file['event']}/{file['source']}/{file['name']}")

    return res


def _show_pipelines(collection) -> list[str]:
    return collection.distinct("origin")


def _show_pipeline_events(collection, pipeline: str) -> list[str]:
    return collection.distinct("event", {"origin": pipeline})


def _show_pipeline_stages(collection, pipeline: str, event: str) -> list[str]:
    return collection.distinct("source", {
        "origin": pipeline,
        "event": event
    })


def _show_pipeline_stage_files(collection, pipeline: str, event: str, stage: str) -> list[str]:
    return collection.distinct("name", {
            "origin": pipeline,
            "event": event,
            "source": stage,
    })


def _query_file(collection, pipeline: str, event: str, stage: str, name: str):
    return collection.find_one({
        "origin": pipeline,
        "event": event,
        "source": stage,
        "name": name,
    })


def _serve_file(file, origin, event, source, name, file_type):
    match file_type:
        case "bin":
            file = File(
                canonical_path=CanonicalPath(
                    origin=origin,
                    event=event,
                    source=source,
                    name=name,
                ),
                data=dill.loads(file["data"]),
                metadata=file["metadata"],
                file_type=file["filetype"],
                description=file["description"]
            )

            file_stream = io.BytesIO(dill.dumps(file, protocol=dill.HIGHEST_PROTOCOL))
            file_stream.seek(0)

            return send_file(file_stream, as_attachment=True, download_name=f"{name}.{file_type}")

        case "plot":
            data: TimeSeries = dill.loads(file["data"])

            return _create_bokeh_plot(data, name)

        case _:
            return "Invalid File Type!", 404


def get_file(collection, path, request_args):
    path_parts = path.split('/') if path else []

    match len(path_parts):
        case 0:     # Top-level directories
            results = _show_pipelines(collection)
            return render_template("list.html", items=results, path=path, title="Pipelines")

        case 1:     # User is querying the events that a pipeline processed
            return render_template(
                "list.html",
                items=_show_pipeline_events(collection, path_parts[0]),
                path=path,
                title=f"Events of {path_parts[0]}"
            )

        case 2:     # User is querying the stages that were processed for an event and pipeline
            return render_template(
                "list.html",
                items=_show_pipeline_stages(collection, path_parts[0], path_parts[1]),
                path=path,
                title=f"Stages of {path_parts[0]}/{path_parts[1]}"
            )

        case 3:     # User is querying the files produced by a stage for an event and pipeline
            return render_template(
                "list.html",
                items=_show_pipeline_stage_files(collection, path_parts[0], path_parts[1], path_parts[2]),
                path=path,
                title=f"Stages of {path_parts[0]}/{path_parts[1]}"
            )

        case 4:
            file = _query_file(collection, path_parts[0], path_parts[1], path_parts[2], path_parts[3])

            # User is querying the page looking at the high-level file details
            if "file_type" not in request.args.keys():
                return render_template(
                    'access.html',
                    file_types=["bin", "plot"],
                    file_name=path_parts[3]
                )

            # User is trying to download file data in a certain form
            else:
                return _serve_file(
                    file,
                    path_parts[0],
                    path_parts[1],
                    path_parts[2],
                    path_parts[3],
                    request_args.get("file_type")
                )

        case _:
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
