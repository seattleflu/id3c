"""
Routes for barcode scans during sample processing.
"""
import logging
from flask import Blueprint, request
from typing import Callable
from .. import datastore
from ..utils.routes import authentication_required, content_types_accepted, check_content_length


LOG = logging.getLogger(__name__)

blueprint = Blueprint("scan", __name__)


@blueprint.route("/scan", methods = ['POST'])
@content_types_accepted(["application/json"])
@check_content_length
@authentication_required
def receive_scan():
    """
    Receive a new set of scanned barcodes.

    POST /scan with a standard form body or JSON body containing the keys
    ``collection`` (optional, scalar), ``sample`` (required, scalar), and
    ``aliquots`` (required, list).
    """
    session = datastore.login(
        username = request.authorization.username,
        password = request.authorization.password)

    scan = request.get_json(force = True)

    LOG.debug(f"Received scan {scan}")

    datastore.store_scan(session, scan)

    return "", 204
