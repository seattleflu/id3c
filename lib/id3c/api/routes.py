"""
API route definitions.
"""
import json
from jsonschema import Draft7Validator, ValidationError, draft7_format_checker
import logging
import pkg_resources
from flask import Blueprint, jsonify, request, send_file
from datetime import datetime
from hashlib import sha1
from . import datastore
from .utils.routes import authenticated_datastore_session_required, content_types_accepted, check_content_length
from . import schemas

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

    LOG.debug(f"Received enrollment")

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
    document = request.form.to_dict()

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

@api_v1.route("/verification/barcode-uses/verify", methods=['POST'])
@content_types_accepted(["application/json"])
@check_content_length
@authenticated_datastore_session_required
def verify_barcode_uses(*, session):
    """
    Validate a list of barcodes and use types.
    
    POST /barcode-uses with a JSON array of objects containing ``barcode`` and
    ``use`` keys with string values.

    Response will be a JSON array in the same order as input array, with objects containing
    ``barcode`` (string) and ``use`` (string) from input array plus ``barcode_found`` (boolean)
    to indicate if the barcode exists in the database, and ``use_match`` (boolean) to indicate
    if the use from the input matches what is stored in the database (the ``use_match`` value
    will be `null` if ``barcode_found`` is `false`).
    """
    barcode_use_list = request.get_json()

    LOG.debug(f"Validating «{barcode_use_list}»")

    try:
        Draft7Validator(schemas.VERIFY_BARCODE_USES_SCHEMA).validate(barcode_use_list)
    except ValidationError as e:
        return str(e), 400
    
    result = datastore.verify_barcode_use_list(session, barcode_use_list)
    return jsonify([ row._asdict() for row in result ])
    
@api_v1.route("/warehouse/identifier/<id>", methods = ['GET'])
@authenticated_datastore_session_required
def get_identifier(id, *, session):
    """
    Retrieve an identifier's metadata.

    GET /warehouse/identifier/*id* to receive a JSON object containing the
    identifier's record.  *id* may be a full UUID or shortened barcode.
    """
    LOG.debug(f"Fetching identifier «{id}»")

    identifier = datastore.fetch_identifier(session, id)

    return jsonify(identifier._asdict())


@api_v1.route("/warehouse/identifier-sets", methods = ['GET'])
@authenticated_datastore_session_required
def get_identifier_sets(*, session):
    """
    Retrieve metadata about all identifier sets.

    GET /warehouse/identifier-set to receive a JSON array of objects, each
    containing a set's metadata fields.
    """
    LOG.debug(f"Fetching identifier sets")

    sets = datastore.fetch_identifier_sets(session)

    return jsonify([ set._asdict() for set in sets ])


@api_v1.route("/warehouse/identifier-sets/<name>", methods = ['GET'])
@authenticated_datastore_session_required
def get_identifier_set(name, *, session):
    """
    Retrieve an identifier set's metadata.

    GET /warehouse/identifier-set/*name* to receive a JSON object containing the set's
    metadata fields.
    """
    LOG.debug(f"Fetching identifier set «{name}»")

    set = datastore.fetch_identifier_set(session, name)

    return jsonify(set._asdict())


@api_v1.route("/warehouse/identifier-sets/<name>", methods = ['PUT'])
@content_types_accepted(["application/x-www-form-urlencoded", "multipart/form-data", None])
@check_content_length
@authenticated_datastore_session_required
def put_identifier_set(name, *, session):
    """
    Make a new identifier set.

    PUT /warehouse/identifier-sets/*name* to create the set if it doesn't yet exist.

    For new sets, *use* form parameter is required. For existing sets, if *use* form parameter
    is provided, its value is updated in the database. Valid *use* values can be found via
    GET /warehouse/identifier-set-uses. 
    
    If a *description* form parameter is provided, its value is set/updated in the database.

    201 Created is returned when the set is created or updated, 204 No
    Content if the set with *name* already existed and was not updated.
    """
    LOG.debug(f"Making identifier set «{name}»")

    fields = {k: v for k, v in request.form.items() if k in ["use","description"]}
    
    new_set = datastore.make_identifier_set(session, name, **fields)

    return "", 201 if new_set else 204

@api_v1.route("/warehouse/identifier-set-uses", methods = ['GET'])
@authenticated_datastore_session_required
def get_identifier_set_uses(*, session):
    """
    Retrieve metadata about all identifier set uses.

    GET /warehouse/identifier-set-uses to receive a JSON array of objects, each
    containing a use's metadata fields.
    """
    LOG.debug(f"Fetching identifier set uses")

    uses = datastore.fetch_identifier_set_uses(session)

    return jsonify([ use._asdict() for use in uses ])

@api_v1.route("/warehouse/sample/<barcode>", methods = ['GET'])
@authenticated_datastore_session_required
def get_sample(barcode, *, session):
    """
    Retrieve a sample's metadata.

    GET /warehouse/sample/*barcode* to receive a JSON object containing the
    sample's record.
    """
    LOG.debug(f"Fetching sample with barcode «{barcode}»")

    sample = datastore.get_sample(session, barcode)

    return jsonify(sample._asdict())

@api_v1.route("/warehouse/sample", methods = ['POST'])
@content_types_accepted(["application/json"])
@check_content_length
@authenticated_datastore_session_required
def post_sample(*, session):
    """
    Create/update a single sample.

    POST /warehouse/sample to validate, then create new or update
    existing sample that passes validation.

    JSON object with status (inserted, updated, or failed validation)
    of the sample is returned.
    """

    sample = request.get_json()

    LOG.debug(f"Creating/updating sample «{sample}»")
    
    # using DraftXValidator(schema).validate(...) instead of jsonschema.validate(...) to
    # return accurate error message for "anyOf" requirement
    try:
        Draft7Validator(schemas.POST_SAMPLE_SCHEMA, format_checker=draft7_format_checker).validate(sample)
    except ValidationError as e:
        return str(e), 400

    # Convert dates in YYYY-MM-DD format to MM/DD/YYYY for consistency with records
    # created through manifest ETL process
    for key in sample:
        if key.endswith('_date'):
            try:
                sample[key] = datetime.strptime(sample[key], '%Y-%m-%d').strftime('%m/%d/%Y')
            except:
                pass
    
    # calculate hash based on serialized sample record
    digest = sha1(json.dumps(sample, sort_keys=True).encode()).hexdigest()

    provenance = {
        "source": request.referrer or request.remote_addr or "?",
        "method": request.method,
        "path": request.path,
        "timestamp": f'{datetime.now():%Y-%m-%d %H:%M:%S%z}',
        "sha1sum": digest
    }
    sample.update(_provenance=provenance)

    # Transform racks data into array to match previously established format
    racks = {key: value for key, value in sample.items() if key.startswith('rack_')}
    if racks:
        sample["racks"]=[]
        for k, v in racks.items():
            sample["racks"].append(sample.pop(k))
    
    # Transform aliquots data into array to match previously established format
    aliquots = {key: value for key, value in sample.items() if key.startswith('aliquot_')}
    if aliquots:
        sample["aliquots"]=[]
        for k, v in aliquots.items():
            sample["aliquots"].append(sample.pop(k))

    result = datastore.store_sample(session, sample)

    status_code = 500
    if "sample" in result:
        status_code = 200
    elif result.get("status") == "validation_failed":
        status_code = 400

    return (jsonify(result), status_code)

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