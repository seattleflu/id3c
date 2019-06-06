"""
Process enrollment documents into the relational warehouse
"""
import click
import logging
from datetime import datetime, timezone
from itertools import groupby
from operator import itemgetter
from typing import Any
from seattleflu.db import find_identifier
from seattleflu.db.session import DatabaseSession
from seattleflu.db.datatypes import Json
from . import etl, find_or_create_site, upsert_individual, upsert_encounter


LOG = logging.getLogger(__name__)


# This revision number is stored in the processing_log of each enrollment
# record when the enrollment is successfully processed by this ETL routine.
# The routine finds new-to-it records to process by looking for enrollments
# lacking this revision number in their log.  If a change to the ETL routine
# necessitates re-processing all enrollments, this revision number should be
# incremented.
REVISION = 4


@etl.command("enrollments", help = __doc__)

@click.option("--dry-run", "action",
    help        = "Only go through the motions of changing the database (default)",
    flag_value  = "rollback",
    default     = True)

@click.option("--prompt", "action",
    help        = "Ask if changes to the database should be saved",
    flag_value  = "prompt")

@click.option("--commit", "action",
    help        = "Save changes to the database",
    flag_value  = "commit")

def etl_enrollments(*, action: str):
    LOG.debug(f"Starting the enrollment ETL routine, revision {REVISION}")

    db = DatabaseSession()

    # XXX TODO: Stop hardcoding valid identifier sets.  Instead, accept them as
    # an option or config (and validate option choices against what's actually
    # in the database).  We won't want to validate using click.option(),
    # because that would necessitate a database connection simply to run
    # bin/id3c at all.
    #   -trs, 13 May 2019
    expected_collection_identifier_sets = {
        "collections-seattleflu.org",
        "collections-fluathome.org",
    }

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
        """, (Json([{ "revision": REVISION }]),))

    processed_without_error = None

    try:
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

                # Find existing collected samples, or create skeletal sample
                # records containing just the collection barcode linked back to
                # this encounter.  Sample manifests generated by the processing
                # lab will usually be loaded later and fill in the rest of the
                # sample record.
                for sample in enrollment.document["sampleCodes"]:
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

                    assert identifier.set_name in expected_collection_identifier_sets, \
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
                        encounter_id          = encounter.id,
                        additional_details    = details)

                # XXX TODO: Should this delete existing linked samples which
                # weren't mentioned in this enrollment document?  This would
                # support the case of an incorrectly included sample from an
                # earlier enrollment document being corrected by a later
                # enrollment document.
                #   -trs, 8 May 2019

                mark_processed(db, enrollment.id)

                LOG.info(f"Finished processing enrollment {enrollment.id}")

    except Exception as error:
        processed_without_error = False

        LOG.error(f"Aborting with error")
        raise error from None

    else:
        processed_without_error = True

    finally:
        if action == "prompt":
            ask_to_commit = \
                "Commit all changes?" if processed_without_error else \
                "Commit successfully processed enrollments up to this point?"

            commit = click.confirm(ask_to_commit)
        else:
            commit = action == "commit"

        if commit:
            LOG.info(
                "Committing all changes" if processed_without_error else \
                "Committing successfully processed enrollments up to this point")
            db.commit()

        else:
            LOG.info("Rolling back all changes; the database will not be modified")
            db.rollback()


def site_details(site: dict) -> dict:
    """
    Describe site details in a simple data structure designed to be used from
    SQL.
    """
    return {
        "type": site.get("type"),
    }


def age(document: dict) -> str:
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
        "locations": encounter_locations(document), # XXX TODO: Model this relationally soon
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

    use_of = itemgetter("use")

    duplicate_uses = [
        use for use, locations
             in groupby(sorted(locations, key = use_of), key = use_of)
             if len(list(locations)) > 1
    ]

    assert not duplicate_uses, \
        f"Document {document['id']} contains more than one location for uses: {duplicate_uses}"

    return {
        location["use"].lower(): location
            for location in locations
    }


def upsert_sample(db: DatabaseSession,
                  collection_identifier: str,
                  encounter_id: int,
                  additional_details: dict) -> Any:
    """
    Upsert collected sample by its *collection_identifier*.

    The provided *additional_details* are merged (at the top-level only) into
    the existing sample details, if any.
    """
    LOG.debug(f"Upserting sample collection «{collection_identifier}»")

    data = {
        "collection_identifier": collection_identifier,
        "encounter_id": encounter_id,
        "additional_details": Json(additional_details),
    }

    sample = db.fetch_row("""
        insert into warehouse.sample (collection_identifier, encounter_id, details)
            values (%(collection_identifier)s, %(encounter_id)s, %(additional_details)s)

        on conflict (collection_identifier) do update
            set encounter_id = excluded.encounter_id,
                details = coalesce(sample.details, '{}') || %(additional_details)s

        returning sample_id as id, identifier, collection_identifier, encounter_id
        """, data)

    assert sample.id, "Upsert affected no rows!"

    LOG.info(f"Upserted sample {sample.id} with collection identifier «{sample.collection_identifier}»")

    return sample


def mark_processed(db, enrollment_id: int) -> None:
    LOG.debug(f"Marking enrollment {enrollment_id} as processed")

    data = {
        "enrollment_id": enrollment_id,
        "log_entry": Json({
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
