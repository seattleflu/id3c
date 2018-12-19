"""
Routes for API root.
"""
from flask import Blueprint, send_file


blueprint = Blueprint("root", __name__)


@blueprint.route("/", methods = ['GET'])
def index():
    """
    Show an index page with documentation.
    """
    return send_file("static/index.html", "text/html; charset=UTF-8")
