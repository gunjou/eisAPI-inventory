from flask import Flask, jsonify
from flask_cors import CORS

from api.endpoints import inventory_bp

api = Flask(__name__)
CORS(api)

api.register_blueprint(inventory_bp)


@api.errorhandler(404)
def page_not_found(e):
    response = {
        "code": e.code,
        "message": e.name
    }
    return jsonify(response), 404
