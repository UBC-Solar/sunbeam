from flask import Flask, render_template, request
from endpoints import commission_pipeline, decommission_pipeline, list_commissioned_pipelines, list_files, get_file
import pymongo


_client = pymongo.MongoClient("mongodb://mongodb:27017/")
_db = _client.sunbeam_db
time_series_collection = _db.time_series_data

app = Flask(__name__)


@app.route("/")
def _index():
    return render_template("index.html")


@app.route("/list_files")
def _list_files():
    return list_files(time_series_collection)


@app.route("/pipelines/decommission_pipeline", methods=['POST', 'GET'])
def _decommission_pipeline():
    if request.method == 'POST':
        git_target = request.form.get('git_target')

        return decommission_pipeline(time_series_collection, git_target)

    else:
        return render_template("decommission.html")


@app.route("/pipelines", methods=['GET'])
def _pipeline():
    return render_template("pipelines.html")


@app.route("/pipelines/commission_pipeline", methods=['GET', 'POST'])
def _commission_pipeline():
    if request.method == 'POST':
        git_target = request.form.get('git_target')

        return commission_pipeline(git_target)

    else:
        return render_template('commission.html')


@app.route("/pipelines/recommission_pipeline", methods=['GET', 'POST'])
def _recommission_pipeline():
    if request.method == 'POST':
        git_target = request.form.get('git_target')

        decommission_pipeline(time_series_collection, git_target)
        commission_pipeline(git_target)

        return f"Recommissioned {git_target}!"

    else:
        return render_template('commission.html')


@app.route('/files', defaults={'path': ''})
@app.route('/files/<path:path>')
def _get_file(path):
    return get_file(time_series_collection, path, request.args)


@app.route("/list_commissioned_pipelines")
def _list_commissioned_pipelines():
    return list_commissioned_pipelines()


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8080)
