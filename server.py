from configparser import ConfigParser

from flask import Flask, request

from db import DB


app = Flask(__name__)
app.config['DEBUG'] = False
app.url_map.strict_slashes = False

configs = ConfigParser()
configs.read("configs.ini")
db_communicator = DB(**configs["db"])


@app.route("/api/events", methods=["GET"])
def get_events():
    start_date = request.args.get("from", None)
    end_date = request.args.get("to", None)
    try:
        data = db_communicator.get_events(start_date, end_date)
        return {'result': data}, 200
    except Exception as err:
        return {'error': err}, 500

@app.errorhandler(404)
def not_found(error):
    return {'error': 'Not Found'}, 404

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=2222)
