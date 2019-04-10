"""
API route definitions.
"""
import logging
from flask import Blueprint, request, send_file
from . import datastore
from .utils.routes import authenticated_datastore_session_required, content_types_accepted, check_content_length


LOG = logging.getLogger(__name__)

api = Blueprint("api", __name__)

blueprints = [
    api,
]


@api.route("/", methods = ['GET'])
def index():
    """
    Show an index page with documentation.
    """
    return send_file("static/index.html", "text/html; charset=UTF-8")


@api.route("/enrollment", methods = ['POST'])
@content_types_accepted(["application/json"])
@check_content_length
@authenticated_datastore_session_required
def receive_enrollment(*, session):
    """
    Receive a new enrollment document.

    POST /enrollment with a JSON body.  Note that we don't actually need to
    parse the JSON body.  The body is passed directly to the database which
    will check its validity.
    """
    document = request.get_data(as_text = True)

    LOG.debug(f"Received enrollment {document}")

    datastore.store_enrollment(session, document)

    return "", 204


@api.route("/scan", methods = ['POST'])
@content_types_accepted(["application/json"])
@check_content_length
@authenticated_datastore_session_required
def receive_scan(*, session):
    """
    Receive a new set of scanned barcodes.

    POST /scan with a JSON body containing the keys ``collection`` (optional,
    scalar), ``sample`` (required, scalar), and ``aliquots`` (required, list).
    """
    scan = request.get_json(force = True)

    LOG.debug(f"Received scan {scan}")

    datastore.store_scan(session, scan)

    return "", 204


@api.route("/presence-absence", methods = ['POST'])
@content_types_accepted(["application/json"])
@check_content_length
@authenticated_datastore_session_required
def receive_presence_absence(*, session):
    """
    Receive new presence/absence data for a set of samples and targets.

    POST /presence-absence with a JSON body.
    """
    document = request.get_data(as_text = True)

    LOG.debug(f"Received presence/absence {document}")

    datastore.store_presence_absence(session, document)

    return "", 204


@api.route("/sequence-read-set", methods = ['POST'])
@content_types_accepted(["application/json"])
@check_content_length
@authenticated_datastore_session_required
def receive_sequence_read_set(*, session):
    """
    Receive references to new sequence reads for a source material.

    POST /sequence-read-set with a JSON body containing an object with a
    ``urls`` key that is an array of URLs (strings).
    """
    document = request.get_data(as_text = True)

    LOG.debug(f"Received sequence read set {document}")

    datastore.store_sequence_read_set(session, document)

    return "", 204
