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
from urllib.parse import urlparse
from urllib.request import urlopen
from fhir.resources.bundle import Bundle, BundleEntry
from fhir.resources.codeableconcept import CodeableConcept
from fhir.resources.coding import Coding
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
from id3c.db import find_identifier, upsert_sample
from id3c.db.session import DatabaseSession
from id3c.db.datatypes import Json
from id3c.utils import getattrpath
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
REVISION = 5
ETL_NAME = 'fhir'
INTERNAL_SYSTEM = 'https://seattleflu.org'
LOCATION_RELATION_SYSTEM = 'http://terminology.hl7.org/CodeSystem/v3-RoleCode'
SNOMED_SYSTEM = 'http://snomed.info/sct'
SNOMED_TERM = 'http://snomed.info/id'
LANGUAGE_SYSTEM = 'urn:ietf:bcp:47'
EXPECTED_COLLECTION_IDENTIFIER_SETS = [
    'collections-household-observation',
    'collections-household-intervention',
    'collections-household-intervention-asymptomatic',
    'collections-swab&send',
    'collections-kiosks',
    'collections-self-test',
    'collections-seattleflu.org',
    'collections-swab&send-asymptomatic',
    'collections-scan',
    'collections-scan-kiosks',
    'collections-uw-home',
    'collections-uw-observed',
    'collections-uw-tiny-swabs',
    'collections-uw-tiny-swabs-home',
    'collections-uw-tiny-swabs-observed',
    'collections-household-general',
    'collections-childcare',
    'collections-scan-tiny-swabs',
    'collections-adult-family-home-outbreak-tiny-swabs',
    'collections-workplace-outbreak-tiny-swabs',
    'collections-airs',
]
EXPECTED_SAMPLE_IDENTIFIER_SETS = ['samples']

@etl.command("fhir", help = __doc__)

@with_database_session
def etl_fhir(*, db: DatabaseSession):
    LOG.debug(f"Starting the FHIR ETL routine, revision {REVISION}")

    # Fetch and iterate over FHIR documents that aren't processed
    #
    # Use a server-side cursor by providing a name and limit to one fetched
    # record at a time, to limit local process size.
    #
    # Rows we fetch are locked for update so that two instances of this
    # command don't try to process the same FHIR documents.
    LOG.debug("Fetching unprocessed FHIR documents")

    fhir_documents = db.cursor("fhir")
    fhir_documents.itersize = 1
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

            # Loop over every Resource the Bundle entry, processing what is
            # needed along the way.
            try:
                assert_required_resource_types_present(resources)
                process_bundle_entries(db, bundle)

            except SkipBundleError as error:
                LOG.warning(f"Skipping bundle in FHIR document «{record.id}»: {error}")
                mark_skipped(db, record.id)
                continue

            mark_processed(db, record.id, {"status": "processed"})
            LOG.info(f"Finished processing FHIR document {record.id}")


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


def process_bundle_entries(db: DatabaseSession, bundle: Bundle):
    """
    Loads Encounter, DiagnosticReport, and other dependent FHIR DomainResources
    from a given *Bundle* into the database warehouse as appropriate for each
    Resource type.
    Ensures that all Encounter resources are processed before DiagnosticReport
    resources to avoid potential :class:`SampleNotFoundError`s.
    """
    for entry in bundle.entry:
        process_encounter_bundle_entry(db, bundle, entry)

    for entry in bundle.entry:
        process_diagnostic_report_bundle_entry(db, bundle, entry)


def process_encounter_bundle_entry(db: DatabaseSession, bundle: Bundle, entry: BundleEntry):
    """
    Given an Encounter resource *entry* from a given *bundle*, processes the
    relevant information into the database.
    """
    resource, resource_type = resource_and_resource_type(entry)

    if resource_type != 'Encounter':
        return

    LOG.debug(f"Processing Encounter Resource «{entry.fullUrl}».")

    related_resources = extract_related_resources(bundle, entry)
    immunization_resources = extract_immunization_resources(bundle)

    encounter = process_encounter(db, resource, related_resources, immunization_resources)

    if not encounter:
        raise SkipBundleError("Insufficient information in Bundle to create an encounter")

    process_encounter_samples(db, resource, encounter.id, related_resources)
    process_locations(db, encounter.id, resource)


def process_diagnostic_report_bundle_entry(db: DatabaseSession, bundle: Bundle, entry: BundleEntry):
    """
    Given an DiagnosticReport resource *entry* from a given *bundle*, processes
    the relevant information into the database.
    """
    resource, resource_type = resource_and_resource_type(entry)

    if resource_type != 'DiagnosticReport':
        return

    LOG.debug(f"Processing DiagnosticReport Resource «{entry.fullUrl}».")

    for reference in resource.specimen:
        barcode = None

        if not reference.identifier:
            specimen = reference.resolved(Specimen)
            barcode = identifier(specimen, f"{INTERNAL_SYSTEM}/sample").strip()

        elif matching_system(reference.identifier, INTERNAL_SYSTEM):
            barcode = reference.identifier.value.strip()

        if not barcode:
            continue

        LOG.debug(f"Looking up collected specimen barcode «{barcode}»")
        specimen_identifier = find_identifier(db, barcode)

        if not specimen_identifier:
            LOG.warning(f"Skipping collected specimen with unknown barcode «{barcode}»")
            continue

        # By default, assume that the incoming barcode is for a collection identifier
        is_collection_identifier = True

        try:
            assert specimen_identifier.set_name in EXPECTED_COLLECTION_IDENTIFIER_SETS, \
                f"Specimen with unexpected «{specimen_identifier.set_name}» barcode «{barcode}»"

        except AssertionError:
            assert specimen_identifier.set_name in EXPECTED_SAMPLE_IDENTIFIER_SETS, \
                f"Specimen with unexpected «{specimen_identifier.set_name}» barcode «{barcode}»"

            is_collection_identifier = False

        sample = find_sample(db, specimen_identifier.uuid)
        if not is_collection_identifier and not sample:
            raise SampleNotFoundError("No sample with identifier «{specimen_identifier.uuid}» found.")

        # Sometimes the Ellume samples come in faster than the specimen manifest
        # is updated. In this case, create a new collection identifier that will
        # be filled in later.
        if not sample:
            LOG.debug(f"Creating sample with collection identifier «{specimen_identifier.uuid}»")

            sample = db.fetch_row("""
                insert into warehouse.sample (collection_identifier)
                    values (%s)
                returning sample_id as id, collection_identifier
                """, (str(specimen_identifier.uuid),))

            LOG.info(f"Created sample {sample.id} with collection identifier «{sample.collection_identifier}»")

        process_presence_absence_tests(db, resource, sample.id, barcode)


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


def extract_immunization_resources(bundle: Bundle) -> Dict[str, List[DomainResource]]:
    """
    Finds all top-level Immunization resources in a given FHIR *bundle* that contain a full
    URL reference to a *patient* identifier. Returns these Resources organized by
    resource type.
    """

    immunization_resources: Dict[str, List[DomainResource]] = defaultdict(list)

    for entry in bundle.entry:
        resource, resource_type = resource_and_resource_type(entry)

        if resource_type == 'Immunization':
            immunization_resources['Immunization'].append(resource)

    return immunization_resources


def assert_required_resource_types_present(resources: Dict[str, List[DomainResource]]):
    """
    Raises an :class:`SkipBundleError` if the given *resources* do not meet the
    following requirements:
    * There is at least one Patient or DiagnosticReport Resource
    * If there is a Patient, there is at least one Encounter
    * The number of Observation Resources equals or exceeds the total number
      of Specimen Resources when there are Encounter Resources. (This is
      required because Observation Resources are the only way to link Specimen
      Resources to Encounter Resources.)
    """
    if not (resources['Patient'] or resources['DiagnosticReport']):
        raise SkipBundleError("Either a Patient or a DiagnosticReport Resource are required in a FHIR Bundle.")

    if resources['Patient'] and not resources['Encounter']:
        raise SkipBundleError("At least one Encounter Resource is required in a FHIR Bundle containing a Patient Resource")

    if resources['Specimen'] and resources['Encounter']:
        specimens = len(resources['Specimen'])
        observations = len(resources['Observation'])

        if not observations >= specimens:
            raise SkipBundleError(
                f"Expected the total number of Observation Resources ({observations}) "
                f"to equal or exceed the total number of Specimen resources "
                f"({specimens}) when Encounter resources are present.")


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
    related_resources: Dict[str, List[DomainResource]],
    patient_related_resources: Dict[str, List[DomainResource]]) -> Optional[Any]:
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

    patient = encounter.subject.resolved(Patient)
    patient_language = process_patient_language(patient)

    patient_details = encounter_details(patient_related_resources)
    individual  = process_patient(db, patient, patient_details)

    contained_resources = extract_contained_resources(encounter)

    encounter_source = process_encounter_source(encounter)
    encounter_reason = process_encounter_reason(encounter)

    part_of_identifier = None
    if encounter.partOf:
        part_of_encounter = encounter.partOf.resolved(Encounter)
        part_of_identifier = identifier(part_of_encounter, f"{INTERNAL_SYSTEM}/encounter")

    # XXX FIXME: This shallow dictionary merge is buggy if there are
    # resources of the same type in both the related and contained sets.
    #   -trs, 19 Dec 2019
    details: Dict[str, Any] = encounter_details({ **related_resources, **contained_resources })
    if patient_language:
        details['language'] = patient_language
    if encounter_source:
        details['_provenance'] = encounter_source
    if encounter_reason:
        details['reason'] = encounter_reason
    if part_of_identifier:
        details['part_of'] = part_of_identifier

    return upsert_encounter(db,
        identifier      = identifier(encounter, f"{INTERNAL_SYSTEM}/encounter"),
        encountered     = encounter.period.start.isostring,
        individual_id   = individual.id,
        site_id         = site.id,
        age             = age,
        details         = details)


def process_encounter_source(encounter: Encounter) -> Any:
    """
    Returns the source of the given *encounter*.

    Expects the source to be a data URI with media type application/json and
    returns the deserialized JSON data.

    If the source is any other kind of URI, it will be returned as-is (as a string).

    >>> _ = lambda source: Encounter({"meta":{"source":source},"status":"finished","class":{}})

    >>> process_encounter_source(_('data:application/json,{"foo":"bar"}'))
    {'foo': 'bar'}

    >>> process_encounter_source(_("data:application/json,%7B%22foo%22%3A%22bar%22%7D"))
    {'foo': 'bar'}

    >>> process_encounter_source(_("data:application/json;base64,eyJmb28iOiJiYXIifQo="))
    {'foo': 'bar'}

    >>> process_encounter_source(_("data:text/plain,someplace somewhere"))
    'data:text/plain,someplace somewhere'

    >>> process_encounter_source(_("https://example.com"))
    'https://example.com'

    >>> process_encounter_source(_('data:application/json,bogus'))
    'data:application/json,bogus'
    """
    if not encounter.meta:
        return None

    encounter_source = encounter.meta.source

    if not encounter_source:
        return None

    try:
        url = urlparse(encounter_source)

        if url.scheme == "data":
            with urlopen(encounter_source) as source:
                content_type = source.headers.get_content_type()

                if content_type == "application/json":
                    return json.load(source)

    except Exception as e:
        LOG.debug(f"Error parsing Encounter.meta.source of «{encounter_source}»", exc_info = e)
        LOG.warning(
            f"Tried to handle Encounter.meta.source of «{encounter_source}», but failed.  "
            f"Returning unparsed string.")

    return encounter_source


def process_patient_language(patient: Patient) -> Optional[str]:
    """
    Returns the preferred langauge code for the given *Patient*
    """
    if not patient.communication:
        return None

    preferred_language = list(filter(lambda c: c.preferred, patient.communication))
    assert len(preferred_language) == 1, "Found more than one preferred language for patient communication"

    return matching_system_code(preferred_language[0].language, LANGUAGE_SYSTEM)


def process_encounter_reason(encounter: Encounter) -> Optional[List[dict]]:
    """
    Returns the coding concept for all the reason codes of an *encounter*.
    """
    if not encounter.reasonCode:
        return None

    return [
        coding.as_json()
        for concept in encounter.reasonCode
        for coding in concept.coding]


def process_patient(db: DatabaseSession, patient: Patient, details: dict) -> Any:
    """
    Returns an upserted individual using data from the given *patient*.
    """
    return upsert_individual(db,
        identifier  = identifier(patient, f"{INTERNAL_SYSTEM}/individual"),
        sex         = sex(patient),
        details     = details)


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

    def related_specimens(encounter: Encounter, resources: Dict[str, List[DomainResource]]) -> Optional[List[Specimen]]:
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

    specimens = related_specimens(encounter, related_resources)
    if not specimens:
        return

    for specimen in specimens:
        barcode = identifier(specimen, f"{INTERNAL_SYSTEM}/sample").strip()

        if not barcode:
            raise Exception("No barcode detectable. Either the barcode identification system is "
                            f"not «{INTERNAL_SYSTEM}/sample», or the barcode value is empty, which "
                            "violates the FHIR docs.")

        LOG.debug(f"Looking up collected specimen barcode «{barcode}»")
        specimen_identifier = find_identifier(db, barcode)

        if not specimen_identifier:
            LOG.warning(f"Skipping collected specimen with unknown barcode «{barcode}»")
            continue

        if not (specimen_identifier.set_name in EXPECTED_COLLECTION_IDENTIFIER_SETS or
                specimen_identifier.set_name in EXPECTED_SAMPLE_IDENTIFIER_SETS):
            LOG.warning(f"Skipping specimen with unexpected «{specimen_identifier.set_name}» barcode «{barcode}»")
            continue

        sample_identifier: str = None
        collection_identifier: str = None
        if specimen_identifier.set_name in EXPECTED_COLLECTION_IDENTIFIER_SETS:
            collection_identifier = specimen_identifier.uuid
        elif specimen_identifier.set_name in EXPECTED_SAMPLE_IDENTIFIER_SETS:
            sample_identifier = specimen_identifier.uuid
        else:
            assert False, "logic bug"

        sample_details = {}
        if specimen.note:
            sample_details['note'] = specimen.note[0].text

        # A Specimen's collection can specify a collectedDateTime or a
        # collectedPeriod.  We don't use the latter currently, but support it
        # since it's not much extra to do so and external FHIR documents could
        # contain it.
        collectedDateTime    = getattrpath(specimen, "collection.collectedDateTime.isostring")
        collectedPeriodStart = getattrpath(specimen, "collection.collectedPeriod.start.isostring")
        collectedPeriodEnd   = getattrpath(specimen, "collection.collectedPeriod.end.isostring")

        collection_date = (
            collectedDateTime or
            collectedPeriodStart or
            collectedPeriodEnd)

        additional_details = { **specimen.type.as_json(), **sample_details}

        # XXX TODO: Improve details object here; the current approach produces
        # an object like {"coding": [{…}]} which isn't very useful.
        upsert_sample(db,
            update_identifiers          = False,
            overwrite_collection_date   = False,
            identifier                  = sample_identifier,
            collection_identifier       = collection_identifier,
            collection_date             = collection_date,
            encounter_id                = encounter_id,
            additional_details          = additional_details)

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
    Returns the value of the first age in months or age in years response from
    a given *questionnaire_response* as an interval.

    Gives precedence to age in months response to preserve specificty
    if available.
    """
    age_in_months: int = None
    age_in_years: int = None

    for item in questionnaire_response.item:
        if item.linkId == 'age_months':
            age_in_months = item.answer[0].valueInteger
        if item.linkId == 'age':
            age_in_years = item.answer[0].valueInteger

    if age_in_months is not None:
        return age(age_in_months / 12)
    elif age_in_years is not None:
        return age(age_in_years)

    return None


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

        identifier = location_reference.location.identifier
        if identifier and matching_system(identifier, f"{INTERNAL_SYSTEM}/site"):
            LOG.debug(f"Site location «{identifier}» will be processed separately")
            continue

        location = location_reference.location.resolved(Location)

        if not location:
            LOG.warning("No reference found to Location resource that was not a site " +
                f"See location: {location_reference.location.as_json()}")
            continue

        process_location(db, encounter_id, location)


def process_location(db: DatabaseSession, encounter_id: int, location: Location):
    """
    Process a FHIR *location* and attach it to an *encounter_id*.
    First looks for a parent location's tract hierarchy;
    if no parent location is available, assumes that the location entry itself
    is a tract and looks for a tract hierarchy.
    """
    # XXX FIXME: This function assumes the scales tract and address and their
    # relationship in the hierarchy instead of being agnostic to the scale.
    #   -trs, 19 Dec 2019

    def get_tract(location: Location) -> Optional[Any]:
        """
        Given a *location*, returns its tract location entry if it exists, else None.
        """
        scale = 'tract'
        tract_identifier = identifier(location, f"{INTERNAL_SYSTEM}/location/{scale}")

        if not tract_identifier:
            return None

        tract = find_location(db, scale, tract_identifier)
        assert tract, f"Tract «{tract_identifier}» is unknown"

        return tract

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
        tract_hierarchy = get_tract(parent_location).hierarchy  # TODO do we only want parent hierarchy?

    except AttributeError:
        LOG.info(f"No parent location found for Location with code «{code}», " + \
            f"relation «{relation}». " + \
            "Assuming location is a tract.")
        try: # handle encounter locations that are only tracts
            tract = get_tract(location)
            tract_hierarchy = tract.hierarchy
        except (AttributeError, AssertionError):
            LOG.info(f"No tract found for Location with code «{code}», " + \
                f"relation «{relation}».")
            tract_hierarchy = None

    address = process_address(location, tract_hierarchy)

    if not (tract_hierarchy or address):
        LOG.warning(f"No tract or address location available for «{relation}»")
        return

    upsert_encounter_location(db,
        encounter_id    = encounter_id,
        relation        = relation,
        location_id     = address.id if address else tract.id)
        # if there is an address, link encounter location to address; if only tract, link encounter location to tract


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


def process_presence_absence_tests(db: DatabaseSession, report: DiagnosticReport,
    sample_id: int, barcode: str):
    """
    Given a *report* containing presence-absence test results, upserts them to
    ID3C, attaching a sample and target ID.
    """
    def observation_value(observation: Observation) -> bool:
        """
        Return the boolean value of a presence/absence result observation.

        Expects the observation value to be within valueBoolean or
        valueCodeableConcept (or None in the case of valueBoolean).

        Also raises Exception if valueCodeableConcept contains an unknown code.
        """
        if observation.valueBoolean is not None:
            return observation.valueBoolean

        elif observation.valueCodeableConcept is not None:
            code_map = {
                "10828004": True,
                "260385009": False,
                "82334004": None,
            }
            code = matching_system_code(observation.valueCodeableConcept, SNOMED_SYSTEM)

            if code not in code_map:
                raise Exception(f"Unknown SNOMED code «{code}»")

            return code_map[code]

        elif observation.valueBoolean is None:
            return observation.valueBoolean


    if not report.result:
        raise Exception("An empty value for `result` violates the FHIR docs.")

    for result in report.result:
        observation = result.resolved(Observation)

        snomed_code = matching_system_code(observation.code, SNOMED_SYSTEM)
        assert snomed_code, "No SNOMED code found"

        # Skip SNOMED based results that are Inconclusive
        if snomed_code == '911000124104':
            continue

        # Most of the time we expect to see existing targets so a
        # select-first approach makes the most sense to avoid useless
        # updates.
        target = find_or_create_target(db,
            identifier  = f"{SNOMED_TERM}/{snomed_code}",
            control     = False)

        result_value = observation_value(observation)

        device = observation.device.identifier.value
        details = { "device": device }

        # Skip Ellume inconclusive results
        if device == 'Ellume' and result_value is None:
            continue

        if report.effectiveDateTime:
            details["effective_datetime"] = report.effectiveDateTime.as_json()

        upsert_presence_absence(db,
            identifier = f'{barcode}/{snomed_code}/{observation.device.identifier.value}',
            sample_id = sample_id,
            target_id = target.id,
            present = result_value,
            details = details)


def mark_skipped(db, fhir_id: int) -> None:
    LOG.debug(f"Marking FHIR document {fhir_id} as skipped")
    mark_processed(db, fhir_id, { "status": "skipped" })


def mark_processed(db, fhir_id: int, entry = {}) -> None:
    LOG.debug(f"Marking FHIR document {fhir_id} as processed")

    data = {
        "fhir_id": fhir_id,
        "log_entry": Json({
            **entry,
            "etl": ETL_NAME,
            "revision": REVISION,
            "timestamp": datetime.now(timezone.utc),
        }),
    }

    with db.cursor() as cursor:
        cursor.execute("""
            update receiving.fhir
               set processing_log = processing_log || %(log_entry)s
             where fhir_id = %(fhir_id)s
            """, data)


class SkipBundleError(Exception):
    pass
