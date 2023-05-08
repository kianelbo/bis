from flask import Flask, request

from db import DB


app = Flask(__name__)
app.url_map.strict_slashes = False

db_communicator = DB(hostname, db_name, collection_name)


@app.route('/api/events', methods=['GET'])
def get_resources():
    try:
        data = db_communicator.get_events()
        return {'result': data}, 200
    except Exception as err:
        return {'error': err}, 500

@app.errorhandler(404)
def not_found(error):
    return {'error': 'Not Found'}, 404

if __name__ == '__main__':
    app.run()
