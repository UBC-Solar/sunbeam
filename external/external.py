from flask import Flask, render_template, request, Response, jsonify
import endpoints
import pymongo


_client = pymongo.MongoClient("mongodb://mongodb:27017/")
_db = _client.sunbeam_db
time_series_collection = _db.time_series_data

app = Flask(__name__)


@app.route("/")
def _index():
    return render_template("index.html")


@app.route("/health")
def health():
    return "Hello World!"


@app.route("/list_files")
def _list_files():
    return endpoints.list_files(time_series_collection)


@app.route("/pipelines/decommission_pipeline", methods=['POST', 'GET'])
def _decommission_pipeline():
    if request.method == 'POST':
        git_target = request.form.get('git_target')

        return endpoints.decommission_pipeline(time_series_collection, git_target)

    else:
        return render_template("decommission.html")


@app.route("/pipelines", methods=['GET'])
def _pipeline():
    return render_template("pipelines.html")


@app.route("/pipelines/commission_pipeline", methods=['GET', 'POST'])
def _commission_pipeline():
    if request.method == 'POST':
        git_target = request.form.get('git_target')

        return endpoints.commission_pipeline(git_target)

    else:
        return render_template('commission.html')


@app.route("/pipelines/recommission_pipeline", methods=['GET', 'POST'])
def _recommission_pipeline():
    if request.method == 'POST':
        git_target = request.form.get('git_target')

        endpoints.decommission_pipeline(time_series_collection, git_target)
        endpoints.commission_pipeline(git_target)

        return f"Recommissioned {git_target}!"

    else:
        return render_template('recommission.html')


@app.route('/files', defaults={'path': ''})
@app.route('/files/<path:path>')
def _get_file(path):
    return endpoints.get_file(time_series_collection, path, request.args)


@app.route("/list_commissioned_pipelines")
def _list_commissioned_pipelines():
    return endpoints.list_commissioned_pipelines()


# @app.route('/cache/get')
# def _get_value():
#     key = request.args.get('key')
#     if key is None:
#         return "Must set the `key` parameter to query cache!", 400
#
#     value = endpoints.get_cache_by_key(key)
#     return Response(value, content_type='application/octet-stream') if value else (f"No item with key {key} exists!", 406)


# @app.route('/cache/set')
# def _set_value():
#     key = request.args.get('key')
#     if key is None:
#         return "Must set the `key` parameter to set cache!", 400
#
#     value = request.args.get('value')
#     if value is None:
#         return "Must set the `value` parameter to set cache!", 400
#
#     return endpoints.set_cache_by_key(key, value), 201
#

# @app.route('/cache/exists')
# def _check_exists():
#     key = request.args.get('key')
#     if key is None:
#         return "Must set the `key` parameter to query cache!", 400
#
#     return endpoints.check_cache_by_key(key), 200


# @app.route('/cache/delete')
# def _delete_key():
#     key = request.args.get('key')
#     if key is None:
#         return "Must set the `key` parameter to delete from cache!", 400
#
#     return endpoints.delete_cache_by_key(key), 200


# @app.route('/cache/keys')
# def _cache_keys():
#     return endpoints.get_cache_keys(), 200


@app.route('/files/distinct', methods=['POST', 'GET'])
def _distinct():
    if request.method == 'POST':
        key = request.form.get('key')
        if key is None:
            return "Must set the `key` parameter to distinct!", 400

        distinct_filter = {}

        origin = request.form.get('origin')
        if origin:
            distinct_filter['origin'] = origin

        source = request.form.get('source')
        if source:
            distinct_filter['source'] = source

        event = request.form.get('event')
        if event:
            distinct_filter['event'] = event

        name = request.form.get('name')
        if name:
            distinct_filter['name'] = name

        return time_series_collection.distinct(key, distinct_filter), 200

    else:
        return render_template("distinct.html")


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8080)
