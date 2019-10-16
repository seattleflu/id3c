"""
Process clinical documents into the relational warehouse.
"""
import click
import logging
import re
from datetime import datetime, timezone
from typing import Any, Mapping, Optional
from id3c.db import find_identifier
from id3c.db.session import DatabaseSession
from id3c.db.datatypes import Json
from . import (
    etl,

    age,
    age_to_delete,
    find_or_create_site,
    find_sample,
    find_location,
    update_sample,
    upsert_encounter,
    upsert_individual,
    upsert_encounter_location,

    SampleNotFoundError,
    UnknownEthnicGroupError,
    UnknownFluShotResponseError,
    UnknownRaceError,
    UnknownSiteError,
)


LOG = logging.getLogger(__name__)


# This revision number is stored in the processing_log of each clinical
# record when the clinical record is successfully processed by this ETL
# routine. The routine finds new-to-it records to process by looking for
# clinical records lacking this revision number in their log.  If a
# change to the ETL routine necessitates re-processing all clinical records,
# this revision number should be incremented.
REVISION = 3


@etl.command("clinical", help = __doc__)

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

def etl_clinical(*, action: str):
    LOG.debug(f"Starting the clinical ETL routine, revision {REVISION}")

    db = DatabaseSession()

    # Fetch and iterate over clinical records that aren't processed
    #
    # Rows we fetch are locked for update so that two instances of this
    # command don't try to process the same clinical records.
    LOG.debug("Fetching unprocessed clinical records")

    clinical = db.cursor("clinical")
    clinical.execute("""
        select clinical_id as id, document
          from receiving.clinical
         where not processing_log @> %s
         order by id
           for update
        """, (Json([{ "revision": REVISION }]),))

    processed_without_error = None

    try:
        for record in clinical:
            with db.savepoint(f"clinical record {record.id}"):
                LOG.info(f"Processing clinical record {record.id}")

                # Check validity of barcode
                received_sample_identifier = sample_identifier(db,
                    record.document["barcode"])

                # Skip row if no matching identifier found
                if received_sample_identifier is None:
                    LOG.info("Skipping due to unknown barcode " + \
                              f"{record.document['barcode']}")
                    mark_skipped(db, record.id)
                    continue

                # Check sample exists in database
                sample = find_sample(db,
                    identifier = received_sample_identifier)

                # Skip row if sample does not exist
                if sample is None:
                    LOG.info("Skipping due to missing sample with identifier " + \
                                f"{received_sample_identifier}")
                    mark_skipped(db, record.id)
                    continue

                # Most of the time we expect to see existing sites so a
                # select-first approach makes the most sense to avoid useless
                # updates.
                site = find_or_create_site(db,
                    identifier = site_identifier(record.document["site"]),
                    details    = {"type": "retrospective"})


                # Most of the time we expect to see new individuals and new
                # encounters, so an insert-first approach makes more sense.
                # Encounters we see more than once are presumed to be
                # corrections.
                individual = upsert_individual(db,
                    identifier  = record.document["individual"],
                    sex         = sex(record.document["AssignedSex"]))

                encounter = upsert_encounter(db,
                    identifier      = record.document["identifier"],
                    encountered     = record.document["encountered"],
                    individual_id   = individual.id,
                    site_id         = site.id,
                    age             = age(record.document),
                    details         = encounter_details(record.document))

                sample = update_sample(db,
                    sample = sample,
                    encounter_id = encounter.id)

                # Link encounter to a Census tract, if we have it
                tract_identifier = record.document.get("census_tract")

                if tract_identifier:
                    # Special-case float-like identifiers in earlier date
                    tract_identifier = re.sub(r'\.0$', '', str(tract_identifier))

                    tract = find_location(db, "tract", tract_identifier)
                    assert tract, f"Tract «{tract_identifier}» is unknown"

                    upsert_encounter_location(db,
                        encounter_id = encounter.id,
                        relation = "residence",
                        location_id = tract.id)

                mark_processed(db, record.id, {"status": "processed"})

                LOG.info(f"Finished processing clinical record {record.id}")

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
                "Commit successfully processed clinical records up to this point?"

            commit = click.confirm(ask_to_commit)
        else:
            commit = action == "commit"

        if commit:
            LOG.info(
                "Committing all changes" if processed_without_error else \
                "Committing successfully processed clinical records up to this point")
            db.commit()

        else:
            LOG.info("Rolling back all changes; the database will not be modified")
            db.rollback()

def site_identifier(site_name: str) -> str:
    """
    Given a *site_name*, returns its matching site identifier.
    """
    if not site_name:
        LOG.debug("No site name found")
        return "Unknown"  # TODO

    site_name = site_name.upper()

    site_map = {
        "UWMC": "RetrospectiveUWMedicalCenter",
        "HMC": "RetrospectiveHarborview",
        "NWH":"RetrospectiveNorthwest",
        "UWNC": "RetrospectiveUWMedicalCenter",
        "SCH": "RetrospectiveChildrensHospitalSeattle",
        "KP": "KaiserPermanente",
    }
    if site_name not in site_map:
        raise UnknownSiteError(f"Unknown site name «{site_name}»")

    return site_map[site_name]

def sex(sex_name) -> str:
    """
    Given a *sex_name*, returns its matching sex identifier.
    """
    if type(sex_name) is str:
        sex_name = sex_name.upper()

    sex_map = {
        "M": "male",
        "F": "female",
        1.0: "male",
        0.0: "female"
    }

    return sex_map.get(sex_name, "other")


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
                "FluShot": flu_shot(document.get("FluShot")),
                "AssignedSex": [sex(document.get("AssignedSex"))],
                "HispanicLatino": hispanic_latino(document.get("HispanicLatino")),
                "MedicalInsurance": insurance(document.get("MedicalInsurance"))
            },
        }


def race(races: Optional[Any]) -> list:
    """
    Given one or more *races*, returns the matching race identifier found in
    Audere survey data.
    """
    if races is None:
        LOG.debug("No race response found.")
        return [None]

    if not isinstance(races, list):
        races = [races]

    race_map = {
        "American Indian or Alaska Native": "americanIndianOrAlaskaNative",
        "amerind": "americanIndianOrAlaskaNative",
        "Asian": "asian",
        "Black or African American": "blackOrAfricanAmerican",
        "black": "blackOrAfricanAmerican",
        "Native Hawaiian or Other Pacific Islander": "nativeHawaiian",
        "nativehi": "nativeHawaiian",
        "White": "white",
        "Multiple races": "other",
        "refused": None,
    }

    def standardize_race(race):
        try:
            return race if race in race_map.values() else race_map[race]
        except KeyError:
            raise UnknownRaceError(f"Unknown race name «{race}»") from None

    return list(map(standardize_race, races))



def hispanic_latino(ethnic_group: Optional[Any]) -> list:
    """
    Given an *ethnic_group*, returns yes/no value for HispanicLatino key.
    """
    if ethnic_group is None:
        LOG.debug("No ethnic group response found.")
        return [None]

    ethnic_map = {
        "Not Hispanic or Latino": "no",
        "Hispanic or Latino": "yes",
        0.0: "no",
        1.0: "yes",
    }

    if ethnic_group not in ethnic_map:
        raise UnknownEthnicGroupError(f"Unknown ethnic group «{ethnic_group}»")

    return [ethnic_map[ethnic_group]]

def flu_shot(flu_shot_response: Optional[Any]) -> list:
    """
    Given a *flu_shot_response*, returns yes/no value for FluShot key.
    """
    if flu_shot_response is None:
        LOG.debug("No flu shot response found.")
        return [None]

    flu_shot_map = {
        0.0 : "no",
        1.0 : "yes"
    }

    if flu_shot_response not in flu_shot_map:
        raise UnknownFluShotResponseError(
            f"Unknown flu shot response «{flu_shot_response}»")

    return [flu_shot_map[flu_shot_response]]

def insurance(insurance_response: Optional[Any]) -> list:
    """
    Given an *insurance_response*, returns corresponding insurance
    identifier.
    """
    if insurance_response is None:
        LOG.debug("No insurance response found.")
        return [None]

    insurance_map = {
        "Commercial": "privateInsurance",
        "Medicaid": "government",
        "Medicare": "government",
        "Tricare": "government",
        "Other": "other"
    }

    return [insurance_map.get(insurance_response, None)]


def sample_identifier(db: DatabaseSession, barcode: str) -> Optional[str]:
    """
    Find corresponding UUID for scanned sample or collection barcode within
    warehouse.identifier.

    Will be sample barcode if from UW and collection barcode if from SCH.
    """
    identifier = find_identifier(db, barcode)

    if identifier:
        assert identifier.set_name == "samples" or \
            identifier.set_name == "collections-seattleflu.org", \
            f"Identifier found in set «{identifier.set_name}», not «samples»"

    return identifier.uuid if identifier else None

def mark_skipped(db, clinical_id: int) -> None:
    LOG.debug(f"Marking clinical record {clinical_id} as skipped")
    mark_processed(db, clinical_id, { "status": "skipped" })


def mark_processed(db, clinical_id: int, entry: Mapping) -> None:
    LOG.debug(f"Marking clinical document {clinical_id} as processed")

    data = {
        "clinical_id": clinical_id,
        "log_entry": Json({
            **entry,
            "revision": REVISION,
            "timestamp": datetime.now(timezone.utc),
        }),
    }

    with db.cursor() as cursor:
        cursor.execute("""
            update receiving.clinical
               set processing_log = processing_log || %(log_entry)s
             where clinical_id = %(clinical_id)s
            """, data)
