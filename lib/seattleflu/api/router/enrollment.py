"""
Routes for the Audere enrollment app backend.
"""
import logging
from flask import Blueprint, request
from .. import datastore
from ..utils.routes import authentication_required, content_types_accepted, check_content_length


LOG = logging.getLogger(__name__)

blueprint = Blueprint("enrollment", __name__)


@blueprint.route("/enrollment", methods = ['POST'])
@content_types_accepted(["application/json"])
@check_content_length
@authentication_required
def receive_enrollment():
    """
    Receive a new enrollment document.

    POST /enrollment with a JSON body.  Note that we don't actually need to
    parse the JSON body.  The body is passed directly to the database which
    will check its validity.
    """
    session = datastore.login(
        username = request.authorization.username,
        password = request.authorization.password)

    document = request.get_data(as_text = True)

    LOG.debug(f"Received enrollment {document}")

    datastore.store_enrollment(session, document)

    return "", 204
