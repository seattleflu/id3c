"""
Process longitudinal documents into the relational warehouse.
"""
import click
import logging
from typing import Any
from datetime import datetime, timezone
from seattleflu.db import find_identifier
from seattleflu.db.session import DatabaseSession
from seattleflu.db.datatypes import Json
from . import (
    etl,

    age,
    age_to_delete,
    find_or_create_site,
    find_sample,
    find_location,
    update_sample,
    upsert_encounter,
    upsert_encounter_location,

    SampleNotFoundError,
    UnknownEthnicGroupError,
    UnknownFluShotResponseError,
    UnknownRaceError,
    UnknownSiteError,
)


LOG = logging.getLogger(__name__)


# This revision number is stored in the processing_log of each longitudinal
# record when the longitudinal record is successfully processed by this ETL
# routine. The routine finds new-to-it records to process by looking for
# longitudinal records lacking this revision number in their log.  If a
# change to the ETL routine necessitates re-processing all longitudinal records,
# this revision number should be incremented.
REVISION = 2


@etl.command("longitudinal", help = __doc__)

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

def etl_longitudinal(*, action: str):
    LOG.debug(f"Starting the longitudinal ETL routine, revision {REVISION}")

    db = DatabaseSession()

    # Fetch and iterate over longitudinal records that aren't processed
    #
    # Rows we fetch are locked for update so that two instances of this
    # command don't try to process the same longitudinal records.
    LOG.debug("Fetching unprocessed longitudinal records")

    longitudinal = db.cursor("longitudinal")
    longitudinal.execute("""
        select longitudinal_id as id, document
          from receiving.longitudinal
         where not processing_log @> %s
         order by id
           for update
        """, (Json([{ "revision": REVISION }]),))

    processed_without_error = None

    try:
        for record in longitudinal:
            with db.savepoint(f"longitudinal record {record.id}"):
                LOG.info(f"Processing longitudinal record {record.id}")

                # Check validity of barcode
                received_sample_identifier = sample_identifier(db,
                                                               record.document)

                # Check sample exists in database
                sample = find_sample(db,
                    identifier = received_sample_identifier)


                # Most of the time we expect to see existing sites so a
                # select-first approach makes the most sense to avoid useless
                # updates.
                site = find_or_create_site(db,
                    identifier = site_identifier(record.document),
                    details    = {"type": record.document['type']})


                # Most of the time we expect to see existing individuals and new
                # encounters.
                # Encounters we see more than once are presumed to be
                # corrections.
                individual = find_or_create_individual(db,
                    identifier  = record.document["individual"],
                    sex         = sex(record.document))

                encounter = upsert_encounter(db,
                    identifier      = record.document["identifier"],
                    encountered     = record.document["encountered"],
                    individual_id   = individual.id,
                    site_id         = site.id,
                    age             = age(record.document),
                    details         = encounter_details(record.document))

                if sample:
                    sample = update_sample(db,
                        sample = sample,
                        encounter_id = encounter.id)

                # Link encounter to a Census tract, if we have it
                tract_identifier = record.document.get("census_tract")

                if tract_identifier:
                    tract = find_location(db, "tract", str(tract_identifier))
                    assert tract, f"Tract «{tract_identifier}» is unknown"

                    upsert_encounter_location(db,
                        encounter_id = encounter.id,
                        relation = "residence",
                        location_id = tract.id)

                mark_processed(db, record.id, {"status": "processed"})

                LOG.info(f"Finished processing longitudinal record {record.id}")

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
                "Commit successfully processed longitudinal records up to this point?"

            commit = click.confirm(ask_to_commit)
        else:
            commit = action == "commit"

        if commit:
            LOG.info(
                "Committing all changes" if processed_without_error else \
                "Committing successfully processed longitudinal records up to this point")
            db.commit()

        else:
            LOG.info("Rolling back all changes; the database will not be modified")
            db.rollback()


def sample_identifier(db: DatabaseSession, document: dict) -> str:
    """
    Given a *document*, find corresponding UUID for scanned sample or collection
    barcode within warehouse.identifier.
    """
    barcode = document.get('barcode')

    identifier = find_identifier(db, barcode)
    set_name = 'collections-seattleflu.org'

    if identifier:
        assert identifier.set_name == set_name, \
            f"Identifier found in set «{identifier.set_name}», not «{set_name}»"

    return str(identifier.uuid) if identifier else None


def site_identifier(document: dict) -> str:
    """
    Given a *document*, parses the site and returns its matching site identifier.
    """
    site_name = document.get('site')

    site_map = {
        1: "HutchKids",
        2: "WestCampusChildCareCenter"
    }
    if site_name not in site_map:
        raise UnknownSiteError(f"Unknown site name «{site_name}»")

    return site_map[site_name]


def encounter_details(document: dict) -> dict:
    """
    Describe encounter details in a simple data structure designed to be used
    from SQL.
    """
    return {
            "age": age_to_delete(document.get("age")), # XXX TODO: Remove age from details

            # XXX TODO: Remove locations from details
            "locations": {
                "home": {
                    "region": document.get("census_tract"),
                }
            },
            "responses": {
                "Race": race(document.get("Race")),
                "FluShot": flu_shot(document),
                "AssignedSex": [sex(document)],
                "HispanicLatino": hispanic_latino(document),
                "MedicalInsurance": insurance(document)
            },
        }


def find_or_create_individual(db: DatabaseSession, identifier: str, sex: str,
                              details: dict=None) -> Any:
    """
    Select indinvidual by *identifier*, or insert it if it doesn't exist.
    """
    LOG.debug(f"Looking up individual «{identifier}»")

    individual = db.fetch_row("""
        select individual_id as id, identifier
          from warehouse.individual
         where identifier = %s
        """, (identifier,))

    if individual:
        LOG.info(f"Found individual {individual.id} «{individual.identifier}»")
    else:
        LOG.debug(f"individual «{identifier}» not found, adding")

        data = {
            "identifier": identifier,
            "sex": sex,
            "details": Json(details),
        }

        individual = db.fetch_row("""
            insert into warehouse.individual (identifier, sex, details)
                values (%(identifier)s, %(sex)s, %(details)s)
            returning individual_id as id, identifier
            """, data)

        LOG.info(f"Created individual {individual.id} «{individual.identifier}»")

    return individual


def sex(document: dict) -> str:
    """
    Given a *document*, parses the sex name and returns its matching sex
    identifier.
    """
    sex_name = document.get("AssignedSex")

    sex_map = {
        1: "male",
        2: "female"
    }

    return sex_map.get(sex_name, "other")


def race(races: list) -> list:
    """
    Given a *race_name*, returns the matching race identifier found in Audere
    survey data.
    """
    if races is None:
        LOG.debug("No race response found.")
        return [None]

    race_map = {
        "native_american": "americanIndianOrAlaskaNative",
        "asian": "asian",
        "black": "blackOrAfricanAmerican",
        "native_hawaiian": "nativeHawaiian",
        "white": "white",
        "other": "other",
        "unknown": None,
    }

    for i in range(len(races)):
        if races[i] not in race_map:
            raise UnknownRaceError(f"Unknown race name «{races[i]}»")
        races[i] = race_map[races[i]]

    return races


def hispanic_latino(document: dict) -> list:
    """
    Given a *document*, returns yes/no value for its HispanicLatino key.
    """
    ethnic_group = document.get("HispanicLatino")

    if ethnic_group is None:
        LOG.debug("No ethnic group response found.")
        return [None]

    ethnic_map = {
        1: "yes",
        2: "no",
        3: None,
    }

    if ethnic_group not in ethnic_map:
        raise UnknownEthnicGroupError(f"Unknown ethnic group «{ethnic_group}»")

    return [ethnic_map[ethnic_group]]


def flu_shot(document: dict) -> list:
    """
    Given a *document*, returns a 'yes', 'no', or 'doNotKnow' value for its
    'FluShot' key.
    """
    flu_shot_response = document.get("FluShot")
    if flu_shot_response is None:
        LOG.debug("No flu shot response found.")
        return [None]

    flu_shot_map = {
        1: "yes",
        2: "no",
        3: "doNotKnow",
    }

    if flu_shot_response not in flu_shot_map:
        raise UnknownFluShotResponseError(
            f"Unknown flu shot response «{flu_shot_response}»")

    return [flu_shot_map[flu_shot_response]]


def insurance(document: dict) -> list:
    """
    Given a dict, parses its insurance response and returns a corresponding
    insurance identifier.
    """
    insurance_response = document.get("MedicalInsurance")

    if insurance_response is None:
        LOG.debug("No insurance response found.")
        return [None]

    insurance_map = {
        1: "privateInsurance",
        2: "governmentInsurance",
        3: "noInsurance",
    }

    return [insurance_map.get(insurance_response, None)]


def mark_processed(db, longitudinal_id: int, entry: {}) -> None:
    LOG.debug(f"Marking longitudinal document {longitudinal_id} as processed")

    data = {
        "longitudinal_id": longitudinal_id,
        "log_entry": Json({
            **entry,
            "revision": REVISION,
            "timestamp": datetime.now(timezone.utc),
        }),
    }

    with db.cursor() as cursor:
        cursor.execute("""
            update receiving.longitudinal
               set processing_log = processing_log || %(log_entry)s
             where longitudinal_id = %(longitudinal_id)s
            """, data)
