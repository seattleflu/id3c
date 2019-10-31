"""
API route definitions.
"""
import json
import logging
from flask import Blueprint, request, send_file, Response, jsonify
from flask_cors import cross_origin
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


@api_v1.route("/receiving/enrollment", methods = ['POST'])
@api_unversioned.route("/enrollment", methods = ['POST'])
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

    LOG.debug(f"Received presence/absence {document}")

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

    LOG.debug(f"Received consensus genome {document}")

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

    LOG.debug(f'Received FHIR document {document}')

    datastore.store_fhir(session, document)

    return "", 204


@api_v1.route("/shipping/augur-build-metadata", methods = ['GET'])
@authenticated_datastore_session_required
def get_metadata(session):
    """
    Export metadata needed for SFS augur build
    """
    LOG.debug("Exporting metadata for SFS augur build")

    metadata = datastore.fetch_metadata_for_augur_build(session)

    return Response((row[0] + '\n' for row in metadata), mimetype="application/x-ndjson")


@api_v1.route("/shipping/genomic-data/<lineage>/<segment>", methods = ['GET'])
@authenticated_datastore_session_required
def get_genomic_data(lineage, segment, session):
    """
    Export genomic data needed for SFS augur build based on provided
    *lineage* and *segment*.

    The *lineage* should be in the full lineage in ltree format
    such as 'Influenza.A.H1N1'
    """
    LOG.debug(f"Exporting genomic data for lineage <{lineage}> and segment <{segment}>")

    sequences = datastore.fetch_genomic_sequences(session, lineage, segment)

    return Response((row[0] + '\n' for row in sequences), mimetype="application/x-ndjson")


@api_v1.route("/shipping/return-results/<barcode>", methods = ['GET'])
@cross_origin(origins=["https://seattleflu.org/"])
@authenticated_datastore_session_required
def get_barcode_results(barcode, session):
    """
    Export presence/absence results for a specific collection *barcode*
    """
    LOG.debug(f"Exporting presence/absence results for <{barcode}>")
    results = datastore.fetch_barcode_results(session, barcode)
    return jsonify(results)
