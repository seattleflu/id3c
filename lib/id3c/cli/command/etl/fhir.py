"""
Process FHIR documents into the relational warehouse
"""
import re
import json
import click
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, List, Dict, Optional, Tuple
from fhir.resources.bundle import Bundle, BundleEntry
from fhir.resources.codeableconcept import CodeableConcept
from fhir.resources.diagnosticreport import DiagnosticReport
from fhir.resources.domainresource import DomainResource
from fhir.resources.encounter import Encounter
from fhir.resources.identifier import Identifier
from fhir.resources.location import Location
from fhir.resources.observation import Observation
from fhir.resources.patient import Patient
from fhir.resources.questionnaireresponse import QuestionnaireResponse
from fhir.resources.specimen import Specimen
from id3c.cli.command import with_database_session
from id3c.db.session import DatabaseSession
from id3c.db.datatypes import Json
from . import (
    etl,

    find_or_create_site,
    find_or_create_target,
    find_location,
    find_sample,
    upsert_individual,
    upsert_encounter,
    upsert_encounter_location,
    upsert_location,
    upsert_sample,
    upsert_presence_absence,

    SampleNotFoundError,
)


LOG = logging.getLogger(__name__)


# The revision number and etl name are stored in the processing_log of each
# FHIR record when the FHIR document is successfully processed by this ETL
# routine. The routine finds new-to-it records to process by looking for FHIR
# documents lacking this etl revision number and etl name in their log.
# If a change to the ETL routine necessitates re-processing all FHIR documents,
# this revision number should be incremented.
# The etl name has been added to allow multiple etls to process the same
# receiving table
REVISION = 1
ETL_NAME = 'fhir'
INTERNAL_SYSTEM = 'https://seattleflu.org'
LOCATION_RELATION_SYSTEM = 'http://terminology.hl7.org/CodeSystem/v3-RoleCode'
TARGET_SYSTEM = 'http://snomed.info/sct'

@etl.command("fhir", help = __doc__)

@with_database_session
def etl_fhir(*, db: DatabaseSession):
    LOG.debug(f"Starting the FHIR ETL routine, revision {REVISION}")

    # Fetch and iterate over FHIR documents that aren't processed
    #
    # Use a server-side cursor by providing a name.  This ensures we limit how
    # much data we fetch at once, to limit local process size.
    #
    # Rows we fetch are locked for update so that two instances of this
    # command don't try to process the same FHIR documents.
    LOG.debug("Fetching unprocessed FHIR documents")

    fhir_documents = db.cursor("fhir")
    fhir_documents.execute("""
        select fhir_id as id, document
          from receiving.fhir
         where not processing_log @> %s
         order by id
           for update
        """, (Json([{ "etl": ETL_NAME, "revision": REVISION }]),))

    for record in fhir_documents:
        with db.savepoint(f"FHIR document {record.id}"):
            LOG.info(f"Processing FHIR document {record.id}")

            assert_bundle_collection(record.document)
            bundle      = Bundle(record.document)
            resources   = extract_resources(bundle)

            try:
                assert_required_resource_types_present(resources)
            except AssertionError:
                LOG.info("FHIR document doesn't meet minimum requirements for processing.")
                mark_skipped(db, record.id)
                continue

            # Loop over every Resource the Bundle entry, processing what is
            # needed along the way.
            try:
                for entry in bundle.entry:
                    resource, resource_type = resource_and_resource_type(entry)

                    if resource_type == 'Encounter':
                        LOG.debug(f"Processing Encounter Resource «{entry.fullUrl}».")

                        related_resources = extract_related_resources(bundle, entry)

                        encounter = process_encounter(db, resource, related_resources)
                        assert encounter, "Insufficient information to create an encounter."

                        process_encounter_samples(db, resource, encounter.id, related_resources)
                        process_locations(db, encounter.id, resource)

                    elif resource_type == 'DiagnosticReport':
                        for reference in resource.specimen:
                            if not matching_system(reference.identifier, INTERNAL_SYSTEM):
                                continue

                            barcode = reference.identifier.value

                            # TODO delete between comments for production
                            # barcode = '6942eef8-da26-4c0f-8f42-8e26437fab67'
                            # XXX

                            sample = process_sample(db, barcode)
                            process_presence_absence_tests(db, resource, sample.id, barcode)

            except AssertionError:
                LOG.warning("Insufficient Encounter information.")
                mark_skipped(db, record.id)
                continue


def assert_bundle_collection(document: Dict[str, Any]):
    """
    Raises an :class:`AssertionError` if the given *document* is not a Bundle
    resource of type 'collection'.
    """
    assert document['resourceType'] == 'Bundle', \
        "Expected FHIR document Resource type to equal Bundle. Instead " + \
        f"received Resource type «{document['resourceType']}»."

    assert document['type'] == 'collection', \
        "Expected FHIR document type to equal collection. Instead received " + \
        f"type «{document['type']}»."


def resource_and_resource_type(entry: BundleEntry) -> Tuple[DomainResource, str]:
    """ Returns the Resource and Resource type of a given Bundle *entry*. """
    return entry.resource, entry.resource.resource_type


def extract_resources(bundle: Bundle) -> Dict[str, List[DomainResource]]:
    """
    Returns all top-level FHIR Resources in a given *bundle*, organized by
    resource type.
    """
    resources: Dict[str, List[DomainResource]] = defaultdict(list)

    for entry in bundle.entry:
        resource, resource_type = resource_and_resource_type(entry)
        resources[resource_type].append(resource)

    return resources


def extract_related_resources(bundle: Bundle, reference_entry: BundleEntry) -> Dict[str, List[DomainResource]]:
    """
    Finds all top-level FHIR Resources in a given *bundle* that contain a full
    URL reference to a *reference_entry*. Returns these Resources organized by
    resource type.
    """
    def is_related_resource(resource: DomainResource, type: str, url: str) -> bool:
        """
        Returns True if the given *resource* has a *type* reference equal to the
        given *url*. Otherwise, returns False.
        """
        reference_map = {
            'Encounter': 'encounter',
            'Patient': 'subject',
        }

        reference = getattr(resource, reference_map[type], None)
        if reference:
            return reference.reference == url

        return False

    resources: Dict[str, List[DomainResource]] = defaultdict(list)

    reference_type = reference_entry.resource.resource_type
    reference_url = reference_entry.fullUrl

    for entry in bundle.entry:
        resource, resource_type = resource_and_resource_type(entry)

        if is_related_resource(resource, reference_type, reference_url):
            resources[resource_type].append(resource)

    return resources


def extract_contained_resources(resource: DomainResource) -> Dict[str, List[DomainResource]]:
    """
    Returns the contained Resources in a given *resource* organized by Resoure type.
    Returns an empty dict if the given *resource* has no contained Resources.
    """
    resources: Dict[str, List[DomainResource]] = defaultdict(list)

    if not resource.contained:
        return resources

    for contained in resource.contained:
        resources[contained.resource_type].append(contained)

    return resources


def assert_required_resource_types_present(resources: Dict[str, List[DomainResource]]):
    """
    Raises an :class:`AssertionError` if the given *resources* do not meet the
    following requirements:
    * At least one Specimen Resource exists in the given *resources*
    * There is at least one Patient or DiagnosticReport Resource
    * The number of Observation Resources equals or exceeds the total number
      of Encounter or Patient Resources.
    """
    def total_resources(resources: Dict[str, List[DomainResource]], resource_type: str) -> int:
        return len(resources[resource_type]) if resources.get(resource_type) else 0

    assert resources.get('Specimen') and len(resources['Specimen']) >= 1, \
        "At least one Specimen Resource is required in a FHIR Bundle."

    assert resources.get('Patient') or resources.get('DiagnosticReport'), \
        "Either a Patient or a DiagnosticReport Resource are required in a FHIR Bundle."

    patients = total_resources(resources, 'Patient')
    encounters = total_resources(resources, 'Encounter')
    observations = total_resources(resources, 'Observation')
    assert  observations >= max([patients, encounters]), "Expected the total number of " + \
        f"Observation Resources ({observations}) to equal or exceed the total number of " + \
        f"Patient or Encounter resources ({patients} or {encounters}, respectively)."


def identifier(resource: DomainResource, system: str=None) -> Optional[str]:
    """
    Returns a *resource* identifier from a specified *system*.

    If no *system* is provided, defaults to returning the first identifier.

    Raises a :class:`AssertionError` if the given *resource* has more than one
    identifier value within the given *system*.
    """
    if system is None:
        return resource.identifier[0].value

    system_identifier = list(filter(lambda id: matching_system(id, system), resource.identifier))
    if not system_identifier:
        return None

    assert len(system_identifier) == 1, \
        f"More than one identifier found for given {resource.resource_type} " \
        f"Resource, system «{system_identifier}»."

    return system_identifier[0].value


def matching_system(identifier: Identifier, system: str) -> bool:
    return identifier.system == system


def sex(patient: Patient) -> str:
    if patient.gender == 'unknown':
        return None

    return patient.gender


def matching_system_code(concept: CodeableConcept, system: str) -> Optional[str]:
    """
    Returns a code from a specified *system* contained within a given *concept*.

    If no code is found for the given *system*, returns None.

    Raises an :class:`AssertionError` if more than one encoding for a *system*
    is found within the given FHIR *concept*.
    """
    system_codes: List[CodeableConcept] = []

    if not concept:
        return None

    system_codes += list(filter(lambda c: matching_system(c, system), concept.coding))

    assert len(system_codes) <= 1, "Multiple encodings found in FHIR concept " + \
        f"«{concept.concept_type}» for system «{system}»."

    if not system_codes:
        return None
    else:
        return system_codes[0].code


def location_relation(code: str) -> str:
    """
    Given a Location code from the FHIR V3 RoleCode CodeSystem terminology,
    returns a mapped code reflecting our internal location relation system
    (one of ``site``, ``work``, ``residence``, ``lodging``, or ``school``).
    """
    location_relation_map = {
        'HUSCS' : 'site',
        'PTRES' : 'residence',
        'PTLDG' : 'lodging',
        'WORK'  : 'work',
        'SCHOOL': 'school',
    }

    if code not in location_relation_map:
        raise Exception(f"Unknown FHIR V3 RoleCode «{code}».")

    return location_relation_map[code]


def process_encounter(db: DatabaseSession, encounter: Encounter,
    related_resources: Dict[str, List[DomainResource]]) -> Optional[Any]:
    """
    Given a FHIR *encounter* Resource, returns a newly upserted encounter from
    ID3C created with metdata from the given *related_resources*.
    """
    age = encounter_age(encounter, related_resources)

    # Find specific resources referenced in Encounter
    site        = process_encounter_site(db, encounter)
    if not site:
        LOG.warning("Encounter site not found.")
        return None

    individual  = process_encounter_individual(db, encounter)

    contained_resources = extract_contained_resources(encounter)

    return upsert_encounter(db,
        identifier      = identifier(encounter, f"{INTERNAL_SYSTEM}/encounter"),
        encountered     = encounter.period.start.isostring,
        individual_id   = individual.id,
        site_id         = site.id,
        age             = age,
        details         = encounter_details({ **related_resources, **contained_resources }))


def process_encounter_individual(db: DatabaseSession, encounter: Encounter) -> Any:
    """
    Returns an upserted individual using data from the given *encounter*'s
    linked Patient Resource.
    """
    patient = encounter.subject.resolved(Patient)

    return upsert_individual(db,
        identifier  = identifier(patient, f"{INTERNAL_SYSTEM}/individual"),
        sex         = sex(patient))


def process_encounter_site(db: DatabaseSession, encounter: Encounter) -> Optional[Any]:
    """
    Returns a found or created ``site`` (per ID3C encounter-relation
    definitions) using the first site found in the given *encounter*'s linked
    Location Resources.
    """
    if not encounter.location:
        return None

    for encounter_location in encounter.location:
        identifier = encounter_location.location.identifier

        if not identifier or not matching_system(identifier, f"{INTERNAL_SYSTEM}/site"):
            continue

        return find_or_create_site(db,
            identifier  = identifier.value,
            details     = {})

    return None


def process_encounter_samples(db: DatabaseSession, encounter: Encounter, encounter_id: int,
    related_resources: Dict[str, List[DomainResource]]):
    """
    Given a dict of *related_resources*, finds Specimens linked to the given
    *encounter*. Linked Specimens are attached the given *encounter_id* via
    newly upserted samples in ID3C.
    """
    def is_related_specimen(observation: Observation, encounter: Encounter) -> bool:
        return bool(observation.encounter) and observation.encounter.resolved(Encounter) == encounter

    def related_specimens(encounter: Encounter, resources: Dict[str, List[DomainResource]]) -> List[Specimen]:
        """
        Given a dict of FHIR *resources*, returns a list of Specimens linked to a given *encounter*.
        """
        observations = resources.get('Observation')
        if not observations:
            return None

        related_observations = list(filter(lambda o: is_related_specimen(o, encounter), observations))
        specimens = list(map(lambda o: o.specimen.resolved(Specimen), related_observations))

        if not specimens:
            LOG.warning("Encounter specimen not found.")
            return None

        return specimens

    for specimen in related_specimens(encounter, related_resources):
        barcode = identifier(specimen, f"{INTERNAL_SYSTEM}/sample")

        if not barcode:
            raise Exception("No barcode detectable. Either the barcode identification system is "
                            f"not «{INTERNAL_SYSTEM}/sample», or the barcode value is empty, which "
                            "violates the FHIR docs.")

        upsert_sample(db,
            collection_identifier   = barcode,
            encounter_id            = encounter_id,
            details                 = specimen.type.as_json())


def encounter_age(encounter: Encounter, resources: Dict[str, List[DomainResource]]) -> Optional[str]:
    """
    Looks up a QuestionnaireResponse from the given *resources* that links to
    the given *encounter*. Returns the ceiling-limited value of the first age
    response as an interval to attach to an encounter in ID3C.
    """
    questionnaires = resources.get('QuestionnaireResponse')
    if not questionnaires:
        LOG.debug(f"No QuestionnaireResponse found linking to Entry.")
        return None

    for resource in questionnaires:
        if resource.encounter.resolved(Encounter) != encounter:
            continue
        age = process_age(resource)
        if age:
            return age

    LOG.debug(f"No age response found in the QuestionnaireResponse Resources.")
    return None


def process_age(questionnaire_response: QuestionnaireResponse) -> Optional[str]:
    """
    Returns the ceiling-limited value of the first age in months response from a
    given *questionnaire_response* as an interval.
    """
    for item in questionnaire_response.item:
        if item.linkId == 'age_months':
            return age(age_ceiling(item.answer[0].valueInteger / 12))

    return None


def age_ceiling(age: float, max_age=85.0) -> float:
    """
    Given an *age*, returns the same *age* unless it exceeds the *max_age*, in
    which case the *max_age* is returned.
    """
    return min(age, max_age)


def age(age: float) -> str:
    """
    Given a QuestionnaireResponse *item*, returns the first age value as a float
    in years.
    """
    return f"{age} years"


def encounter_details(resources: Dict[str, List[DomainResource]]) -> Dict[str, List[str]]:
    """
    Returns a dictionary with the given *resources* converted to JSON format.
    """
    details: Dict[str, List[str]] = {}
    for type in resources:
        details[type] = [ r.as_json() for r in resources[type] ]

    return details


def process_locations(db: DatabaseSession, encounter_id: int, encounter: Encounter):
    """
    Given an *encounter*, processes the linked locations to attach them to a given
    *encounter_id*.
    """
    for location_reference in encounter.location:
        location = location_reference.location.resolved(Location)

        if not location:
            LOG.debug("No reference found to Location reference. If this Location is a site, " + \
            "it will be processed separately.")
            continue

        process_location(db, encounter_id, location)


def process_location(db: DatabaseSession, encounter_id: int, location: Location):
    """
    Process a FHIR *location* and attach it to an *encounter_id*.
    """
    def get_tract_hierarchy(location: Location) -> Optional[str]:
        """
        Given a *location*, returns its tract hierarchy if it exists, else None.
        """
        scale = 'tract'
        tract_identifier = identifier(location, f"{INTERNAL_SYSTEM}/location/{scale}")

        if not tract_identifier:
            return None

        tract = find_location(db, scale, tract_identifier)
        assert tract, f"Tract «{tract_identifier}» is unknown"

        return tract.hierarchy

    def process_address(location: Location, tract_hierarchy: str) -> Optional[Any]:
        """
        Given an address *Location*, upserts it and attaches it to a
        *tract_hierarchy*.
        """
        # If we have an address identifier ("household id"), we upsert a
        # location record for it.  Addresses are not reasonably enumerable, so
        # we don't require they exist.
        scale = 'address'
        address_identifier = identifier(location, f"{INTERNAL_SYSTEM}/location/{scale}")

        if not address_identifier:
            return None

        return upsert_location(db,
            scale       = scale,
            identifier  = address_identifier,
            hierarchy   = tract_hierarchy)


    code = location_code(location)
    relation = location_relation(code)

    try:
        parent_location = location.partOf.resolved(Location)
        tract_hierarchy = get_tract_hierarchy(parent_location)  # TODO do we only want parent hierarchy?

    except AttributeError:
        LOG.info(f"No parent location found for Location with code «{code}», " + \
            f"relation «{relation}».")
        tract_hierarchy = None

    address = process_address(location, tract_hierarchy)

    if not (tract_hierarchy or address):
        LOG.warning(f"No tract or address location available for «{relation}»")
        return

    upsert_encounter_location(db,
        encounter_id    = encounter_id,
        relation        = relation,
        location_id     = address.id)


def location_code(location: Location) -> str:
    """
    Given a *Location* Resource, returns the matching system code for our
    internal, location-relation system.
    """
    codes: List[str] = []
    codes += list(map(lambda t: matching_system_code(t, LOCATION_RELATION_SYSTEM), location.type))

    if not codes:
        return None

    unique_codes = list(set(codes))
    if len(unique_codes) > 1:
        raise Exception(f"Expected only one Location code. Instead received {unique_codes}.")

    return unique_codes[0]


def process_sample(db: DatabaseSession, barcode: str) -> Any:
    """ Given a *barcode*, returns its matching sample from ID3C. """
    sample = find_sample(db, barcode)

    if not sample:
        raise SampleNotFoundError(f"No sample with «{barcode}» found.")

    return sample


def process_presence_absence_tests(db: DatabaseSession, report: DiagnosticReport,
    sample_id: int, barcode: str):
    """
    Given a *report* containing presence-absence test results, upserts them to
    ID3C, attaching a sample and target ID.
    """
    for result in report.result:
        observation = result.resolved(Observation)

        # Most of the time we expect to see existing targets so a
        # select-first approach makes the most sense to avoid useless
        # updates.
        target = find_or_create_target(db,
            identifier  = matching_system_code(observation.code, TARGET_SYSTEM),
            control     = False)  # TODO what do we expect here? Do we expect more controls?

        upsert_presence_absence(db,
            identifier = f'{barcode}/{observation.device.identifier.value}',  # TODO is this correct use of assay?
            sample_id = sample_id,
            target_id = target.id,
            present = True,
            details = {})


def mark_skipped(db, fhir_id: int) -> None:
    LOG.debug(f"Marking FHIR document {fhir_id} as skipped")
    mark_processed(db, fhir_id, { "status": "skipped" })


def mark_processed(db, fhir_id: int, entry = {}) -> None:
    LOG.debug(f"Appending to processing log of FHIR document {fhir_id}")

    data = {
        "manifest_id": fhir_id,
        "log_entry": Json({
            **entry,
            "etl": ETL_NAME,
            "revision": REVISION,
            "timestamp": datetime.now(timezone.utc),
        }),
    }

    with db.cursor() as cursor:
        cursor.execute("""
            update receiving.manifest
               set processing_log = processing_log || %(log_entry)s
             where manifest_id = %(manifest_id)s
            """, data)
