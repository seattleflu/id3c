"""
API route definitions.
"""
import json
import logging
import pkg_resources
from flask import Blueprint, request, send_file
from . import datastore
from .utils.routes import authenticated_datastore_session_required, content_types_accepted, check_content_length


LOG = logging.getLogger(__name__)

api_v1 = Blueprint('api_v1', 'api_v1', url_prefix='/v1')
api_unversioned = Blueprint('api_unversioned', 'api_unversioned', url_prefix='/')

blueprints = [
    api_v1,
    api_unversioned,
]


@api_v1.route("/", methods = ['GET'])
@api_unversioned.route("/", methods = ['GET'])
def index():
    """
    Show an index page with documentation.
    """
    return send_file("static/index.html", "text/html; charset=UTF-8")


@api_v1.route("/receiving/presence-absence", methods = ['POST'])
@api_unversioned.route("/presence-absence", methods = ['POST'])
@content_types_accepted(["application/json"])
@check_content_length
@authenticated_datastore_session_required
def receive_presence_absence(*, session):
    """
    Receive new presence/absence data for a set of samples and targets.

    POST /presence-absence with a JSON body.
    """
    document = request.get_data(as_text = True)

    LOG.debug(f"Received presence/absence")

    datastore.store_presence_absence(session, document)

    return "", 204


@api_v1.route("/receiving/sequence-read-set", methods = ['POST'])
@api_unversioned.route("/sequence-read-set", methods = ['POST'])
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


@api_v1.route("/receiving/consensus-genome", methods = ['POST'])
@content_types_accepted(["application/json"])
@check_content_length
@authenticated_datastore_session_required
def receive_consensus_genome(*, session):
    """
    Receive references to consensus genomes.

    POST receiving/consensus-genome with a JSON body
    """
    document = request.get_data(as_text = True)

    LOG.debug(f"Received consensus genome")

    datastore.store_consensus_genome(session, document)

    return "", 204


@api_v1.route("/receiving/redcap-det", methods = ['POST'])
@content_types_accepted(["application/x-www-form-urlencoded"])
@check_content_length
@authenticated_datastore_session_required
def receive_redcap_det(*, session):
    """
    Receive REDCap data entry triggers.
    """

    # The REDCap payload should have unique keys, hence we create a flat dict
    document = request.form.to_dict(flat=True)

    LOG.debug(f"Received REDCap data entry trigger")

    datastore.store_redcap_det(session, json.dumps(document))

    return "", 204


@api_v1.route("/receiving/fhir", methods = ['POST'])
@content_types_accepted(["application/fhir+json"])
@check_content_length
@authenticated_datastore_session_required
def receive_fhir(*, session):
    """
    Receive JSON representation of FHIR resources.
    """
    document = request.get_data(as_text = True)

    LOG.debug(f'Received FHIR document')

    datastore.store_fhir(session, document)

    return "", 204


# Load all extra API routes from extensions
# Needs to be at the end of route declarations to allow customization of
# existing routes and avoid dependency errors
for extension in pkg_resources.iter_entry_points("id3c.api.routes"):
    if extension.dist:
        dist = f"{extension.dist.project_name} in {extension.dist.location}"
    else:
        dist = "unknown"

    LOG.debug(f"Loading API routes from extension {extension!s} (distribution {dist})")
    extension.load()
