"""
Process enrollment documents into the relational warehouse
"""
import click
import logging
from datetime import datetime, timezone
from operator import itemgetter
from typing import Any
from seattleflu.db.session import DatabaseSession
from seattleflu.db.datatypes import Json
from . import etl


LOG = logging.getLogger(__name__)


# This revision number is stored in the processing_log of each enrollment
# record when the enrollment is successfully processed by this ETL routine.
# The routine finds new-to-it records to process by looking for enrollments
# lacking this revision number in their log.  If a change to the ETL routine
# necessitates re-processing all enrollments, this revision number should be
# incremented.
REVISION = 1


@etl.command("enrollments", help = __doc__)

@click.option("--commit/--dry-run",
    help = "Save changes to the database or only go through the motions",
    default = False,
    show_default = True)

def etl_enrollments(*, commit: bool):
    LOG.debug(f"Starting the enrollment ETL routine, revision {REVISION}")

    db = DatabaseSession()

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
            with db.savepoint():
                LOG.info(f"Processing enrollment {enrollment.id}")

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
                    details         = encounter_details(enrollment.document))

                mark_processed(db, enrollment.id)

                LOG.info(f"Finished processing enrollment {enrollment.id}")

    except Exception as error:
        processed_without_error = False

        LOG.error(f"Aborting with error")
        raise error from None

    else:
        processed_without_error = True

    finally:
        if commit:
            LOG.info(
                "Committing all changes" if processed_without_error else \
                "Committing successfully processed enrollments up to this point")
            db.commit()

        else:
            LOG.info("Rolling back all changes; the database will not be modified")
            db.rollback()


def find_or_create_site(db: DatabaseSession, identifier: str, details: dict) -> Any:
    """
    Select enrollment site by *identifier*, or insert it if it doesn't exist.
    """
    LOG.debug(f"Looking up site «{identifier}»")

    site = db.fetch_row("""
        select site_id as id, identifier
          from warehouse.site
         where identifier = %s
        """, (identifier,))

    if site:
        LOG.info(f"Found site {site.id} «{site.identifier}»")
    else:
        LOG.debug(f"Site «{identifier}» not found, adding")

        data = {
            "identifier": identifier,
            "details": Json(details),
        }

        site = db.fetch_row("""
            insert into warehouse.site (identifier, details)
                values (%(identifier)s, %(details)s)
            returning site_id as id, identifier
            """, data)

        LOG.info(f"Created site {site.id} «{site.identifier}»")

    return site


def site_details(site: dict) -> dict:
    """
    Describe site details in a simple data structure designed to be used from
    SQL.
    """
    return {
        "type": site["type"],
    }


def upsert_individual(db: DatabaseSession, identifier: str, sex: str = None) -> Any:
    """
    Upsert enrolled individual by their *identifier*.
    """
    LOG.debug(f"Upserting individual «{identifier}»")

    data = {
        "identifier": identifier,
        "sex": sex,
    }

    individual = db.fetch_row("""
        insert into warehouse.individual (identifier, sex)
            values (%(identifier)s, %(sex)s)

        on conflict (identifier) do update
            set sex = excluded.sex

        returning individual_id as id, identifier
        """, data)

    assert individual.id, "Upsert affected no rows!"

    LOG.info(f"Upserted individual {individual.id} «{individual.identifier}»")

    return individual


def upsert_encounter(db: DatabaseSession,
                     identifier: str,
                     encountered: str,
                     individual_id: int,
                     site_id: int,
                     details: dict) -> Any:
    """
    Upsert encounter by its *identifier*.
    """
    LOG.debug(f"Upserting encounter «{identifier}»")

    data = {
        "identifier": identifier,
        "encountered": encountered,
        "individual_id": individual_id,
        "site_id": site_id,
        "details": Json(details),
    }

    encounter = db.fetch_row("""
        insert into warehouse.encounter (
                identifier,
                individual_id,
                site_id,
                encountered,
                details)
            values (
                %(identifier)s,
                %(individual_id)s,
                %(site_id)s,
                %(encountered)s::timestamp with time zone,
                %(details)s)

        on conflict (identifier) do update
            set individual_id = excluded.individual_id,
                site_id       = excluded.site_id,
                encountered   = excluded.encountered,
                details       = excluded.details

        returning encounter_id as id, identifier
        """, data)

    assert encounter.id, "Upsert affected no rows!"

    LOG.info(f"Upserted encounter {encounter.id} «{encounter.identifier}»")

    return encounter


def encounter_details(document: dict) -> dict:
    """
    Describe encounter details in a simple data structure designed to be used
    from SQL.

    Interpreting the contained question → answer ``responses`` map may require
    the data dictionary.
    """
    return {
        "age": document.get("age"),                 # XXX TODO: Model this relationally soon
        "collections": document["sampleCodes"],     # XXX TODO: Model this relationally soon
        "locations": document["locations"],         # XXX TODO: Model this relationally soon
        "language": document["localeLanguageCode"],
        "responses": {
            response["question"]["token"]: decode_answer(response)
                for response in document["responses"]
        },
    }


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
    try:
        return response("AssignedSex", document)
    except NoSuchQuestionError:
        try:
            return response("AssignedSexAirport", document)
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
        chosen_options = answer["chosenOptions"]
        option_tokens = [
            option["token"]
                for option in response_data["options"] ]

        return itemgetter(*chosen_options)(option_tokens)

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
