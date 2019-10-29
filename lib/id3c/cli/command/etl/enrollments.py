"""
Process enrollment documents into the relational warehouse
"""
import click
import logging
from datetime import datetime, timezone
from itertools import groupby
from operator import itemgetter
from typing import Any, Optional
from id3c.cli.command import with_database_session
from id3c.db import find_identifier
from id3c.db.session import DatabaseSession
from id3c.db.datatypes import Json
from . import (
    etl,

    find_or_create_site,
    find_location,
    upsert_individual,
    upsert_encounter,
    upsert_location,
    upsert_encounter_location,
    upsert_sample
)


LOG = logging.getLogger(__name__)


# The revision number and etl name are stored in the processing_log of each
# enrollment record when the enrollment is successfully processed by
# this ETL routine. The routine finds new-to-it records to process by looking
# for enrollments lacking this etl revision number and etl name in their log.
# If a change to the ETL routine necessitates re-processing all enrollments,
# this revision number should be incremented.
# The etl name has been added to allow multiple etls to process the same
# receiving table
REVISION = 5
ETL_NAME = "enrollments"


# XXX TODO: Stop hardcoding valid identifier sets.  Instead, accept them as
# an option or config (and validate option choices against what's actually
# in the database).  We won't want to validate using click.option(),
# because that would necessitate a database connection simply to run
# bin/id3c at all.
#   -trs, 13 May 2019
EXPECTED_COLLECTION_IDENTIFIER_SETS = {
    "collections-seattleflu.org",
    "collections-fluathome.org",
}


@etl.command("enrollments", help = __doc__)
@with_database_session

def etl_enrollments(*, db: DatabaseSession):
    LOG.debug(f"Starting the enrollment ETL routine, revision {REVISION}")

    # Fetch and iterate over enrollments that aren't processed
    #
    # Use a server-side cursor by providing a name.  This ensures we limit how
    # much data we fetch at once, to limit local process size.  Each enrollment
    # document is ~10 KB and the default fetch size (cursor.itersize) is 2,000,
    # thus we'll get ~20 MB on each fetch of 2,000 enrollments.
    #
    # Rows we fetch are locked for update so that two instances of this
    # command don't try to process the same enrollments.
    LOG.debug("Fetching unprocessed enrollments")

    enrollments = db.cursor("enrollments")
    enrollments.execute("""
        select enrollment_id as id, document
          from receiving.enrollment
         where not processing_log @> %s
         order by id
           for update
        """, (Json([{ "etl": ETL_NAME, "revision": REVISION }]),))

    for enrollment in enrollments:
        with db.savepoint(f"enrollment {enrollment.id}"):
            LOG.info(f"Processing enrollment {enrollment.id}")

            # Out of an abundance of caution, fail when the schema version
            # of the enrollment document changes.  This ensures manual
            # intervention happens on document structure changes.  After
            # seeing how versions are handled over time, this restriction
            # may be toned down a bit.
            known_versions = {"1.1.0", "1.0.0"}

            assert enrollment.document["schemaVersion"] in known_versions, \
                f"Document schema version {enrollment.document['schemaVersion']} is not in {known_versions}"

            # Most of the time we expect to see existing sites so a
            # select-first approach makes the most sense to avoid useless
            # updates.
            site = find_or_create_site(db,
                identifier = enrollment.document["site"]["name"],
                details    = site_details(enrollment.document["site"]))

            # Most of the time we expect to see new individuals and new
            # encounters, so an insert-first approach makes more sense.
            # Encounters we see more than once are presumed to be
            # corrections.
            individual = upsert_individual(db,
                identifier  = enrollment.document["participant"],
                sex         = assigned_sex(enrollment.document))

            encounter = upsert_encounter(db,
                identifier      = enrollment.document["id"],
                encountered     = enrollment.document["startTimestamp"],
                individual_id   = individual.id,
                site_id         = site.id,
                age             = age(enrollment.document),
                details         = encounter_details(enrollment.document))

            process_samples(db, encounter.id, enrollment.document)
            process_locations(db, encounter.id, enrollment.document)

            mark_processed(db, enrollment.id)

            LOG.info(f"Finished processing enrollment {enrollment.id}")


def process_samples(db: DatabaseSession,
                    encounter_id: int,
                    document: dict):
    """
    Process an enrollment *document*'s samples.

    Find existing collected samples, or create skeletal sample records
    containing just the collection barcode linked back to this *encounter_id*.
    Sample manifests generated by the processing lab will usually be loaded
    later and fill in the rest of the sample record.
    """
    for sample in document["sampleCodes"]:
        barcode = sample.get("code")

        if not barcode:
            LOG.warning(f"Skipping collected sample with no barcode")
            continue

        # XXX TODO: Stop hardcoding this and handle other types.
        # ScannedSelfSwab and ManualSelfSwabbed are kit barcodes,
        # not collection barcodes.  TestStrip is an identifier
        # UUID, not barcode.
        #   - trs, 17 May 2019
        if sample["type"] != "ClinicSwab":
            LOG.warning(f"Skipping collected sample with unknown type {sample['type']}")
            continue

        LOG.debug(f"Looking up collected sample code «{barcode}»")
        identifier = find_identifier(db, barcode)

        if not identifier:
            LOG.warning(f"Skipping collected {sample['type']} sample with unknown barcode «{barcode}»")
            continue

        assert identifier.set_name in EXPECTED_COLLECTION_IDENTIFIER_SETS, \
            f"{sample['type']} sample with unexpected «{identifier.set_name}» barcode «{barcode}»"

        # XXX TODO: Relationally model sample type after we choose
        # a standard vocabulary (LOINC or SNOMED or whatever FHIR
        # normalizes?)
        #   -trs, 8 May 2019
        details = {
            "type": sample["type"],
        }

        sample = upsert_sample(db,
            collection_identifier = identifier.uuid,
            encounter_id          = encounter_id,
            details               = details)

    # XXX TODO: Should this delete existing linked samples which
    # weren't mentioned in this enrollment document?  This would
    # support the case of an incorrectly included sample from an
    # earlier enrollment document being corrected by a later
    # enrollment document.
    #   -trs, 8 May 2019


def process_locations(db: DatabaseSession, encounter_id: int, document: dict):
    """
    Process an enrollment *document*'s locations and attach them to *encounter_id*.
    """
    locations = encounter_locations(document)

    for (use, location) in locations.items():
        # Find the tract, if we know it.  Tracts are reasonably enumerable, so
        # we require that they already exist.
        tract_identifier = location.get("region")

        if tract_identifier:
            tract = find_location(db, "tract", tract_identifier)
            assert tract, f"Tract «{tract_identifier}» is unknown"
        else:
            tract = None

        # If we have an address identifier ("household id"), we upsert a
        # location record for it.  Addresses are not reasonably enumerable, so
        # we don't require they exist.
        address_identifier = location.get("id")

        if address_identifier:
            address = upsert_location(db,
                scale = "address",
                identifier = address_identifier,
                hierarchy = tract.hierarchy if tract else None)
        else:
            address = None

        if not (tract or address):
            LOG.warning(f"No tract or address location available for «{use}»")
            continue

        # Audere calls this "use", but I think "relation" is a more appropriate
        # term.  We map to preferred nomenclature based loosely on FHIR.
        relation = {
            "home": "residence",
            "work": "workplace",
            "temp": "lodging",
        }

        upsert_encounter_location(db,
            encounter_id = encounter_id,
            relation = relation[use],
            location_id = address.id if address else tract.id)


def site_details(site: dict) -> dict:
    """
    Describe site details in a simple data structure designed to be used from
    SQL.
    """
    return {
        "type": site.get("type"),
    }


def age(document: dict) -> Optional[str]:
    """
    Retrieves the age of the individual at the time of encounter from
    *document*.

    Converts age value from int to string to fit interval format.
    """
    age_dict = document.get("age")
    if not age_dict:
        return None
    if age_dict.get("ninetyOrAbove"):
        return "90 years"
    age = float(age_dict.get("value"))
    # XXX TODO: Determine how Audere will send age in months for < 1 year olds.
    return f"{age} years"


def encounter_details(document: dict) -> dict:
    """
    Describe encounter details in a simple data structure designed to be used
    from SQL.

    Interpreting the contained question → answer ``responses`` map may require
    the data dictionary.
    """
    return {
        "age": document.get("age"),                 # XXX TODO: Remove age from details
        "locations": encounter_locations(document), # XXX TODO: Remove locations from details
        "language": document["localeLanguageCode"],
        "responses": {
            response["question"]["token"]: decode_answer(response)
                for response in document["responses"]
        },
    }


def encounter_locations(document: dict) -> dict:
    """
    Return the encounter *document*'s locations array as a dictionary keyed by
    lowercase location use (``home``, ``work``, ``temp``).

    Raises an :class:`AssertionError` if there's more than one location for a
    use type.
    """
    locations = document["locations"]

    def use_of(location):
        return location["use"].lower()

    duplicate_uses = [
        use for use, locations
             in groupby(sorted(locations, key = use_of), key = use_of)
             if len(list(locations)) > 1
    ]

    assert not duplicate_uses, \
        f"Document {document['id']} contains more than one location for uses: {duplicate_uses}"

    return {
        use_of(location): location
            for location in locations
    }


def mark_processed(db, enrollment_id: int) -> None:
    LOG.debug(f"Marking enrollment {enrollment_id} as processed")

    data = {
        "enrollment_id": enrollment_id,
        "log_entry": Json({
            "etl": ETL_NAME,
            "revision": REVISION,
            "timestamp": datetime.now(timezone.utc),
        }),
    }

    with db.cursor() as cursor:
        cursor.execute("""
            update receiving.enrollment
               set processing_log = processing_log || %(log_entry)s
             where enrollment_id = %(enrollment_id)s
            """, data)


def assigned_sex(document: dict) -> Any:
    """
    Response value of one of the two questions about assigned sex, or None if
    neither question is present in the *document* responses.
    """
    def first_or_none(items):
        return items[0] if items else None

    try:
        return first_or_none(response("AssignedSex", document))
    except NoSuchQuestionError:
        try:
            return first_or_none(response("AssignedSexAirport", document))
        except NoSuchQuestionError:
            LOG.warning(f"No assigned sex response found in document {document['id']}")
            return None


def response(question_id: str, document: dict) -> Any:
    """
    Response value for *question_id* in the enrollment *document*.

    Returns a string, number, tuple of strings, or None.

    Raises a :class:`NoSuchQuestionError` if *question_id* is not found in the
    responses contained by *document*.

    Raises a :class:`TooManyResponsesError` if the *question_id* is not unique
    among responses contained by *document*.
    """
    responses = [
        response
            for response in document["responses"]
             if response["question"]["token"] == question_id ]

    if not responses:
        raise NoSuchQuestionError(f"No question with id/token '{question_id}' in document {document['id']}")

    if len(responses) > 1:
        raise TooManyResponsesError(f"Question id/token '{question_id}' is not unique in responses of document {document['id']}")

    return decode_answer(responses[0])


def decode_answer(response_data: dict) -> Any:
    """
    Decode the answer described by *response_data*, a substructure of an
    enrollment document.

    Returns a string, number, tuple of strings, or None.
    """
    answer = response_data["answer"]

    if answer["type"] in ["String", "Number"]:
        return answer["value"]

    elif answer["type"] == "Option":
        chosen_options = map(int, answer["chosenOptions"])
        option_tokens = [
            option["token"]
                for option in response_data["options"] ]

        return tuple(
            option_tokens[chosen]
                for chosen in chosen_options)

    elif answer["type"] == "Declined":
        return None

    else:
        raise ValueError(f"Unknown response answer type {answer['type']}")


class NoSuchQuestionError(ValueError):
    """
    Raised by :function:`response` if its provided *question_id* is not found
    in the set of responses.
    """
    pass

class TooManyResponsesError(ValueError):
    """
    Raised by :function:`response` if its provided *question_id* is not unique
    among the set of responses.
    """
    pass
