"""
Process clinical documents into the relational warehouse.
"""
import click
import logging
from math import ceil
from datetime import datetime, timezone
from typing import Any
from seattleflu.db.session import DatabaseSession
from seattleflu.db.datatypes import Json
from . import etl
from .enrollments import find_or_create_site, upsert_individual, upsert_encounter


LOG = logging.getLogger(__name__)


# This revision number is stored in the processing_log of each clinical
# record when the clinical record is successfully processed by this ETL 
# routine. The routine finds new-to-it records to process by looking for
# clinical records lacking this revision number in their log.  If a 
# change to the ETL routine necessitates re-processing all clinical records, 
# this revision number should be incremented.
REVISION = 1


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

                # Most of the time we expect to see existing sites so a
                # select-first approach makes the most sense to avoid useless
                # updates.
                site = find_or_create_site(db,
                    identifier = site_identifier(record.document["site"]),
                    details    = {"type": "hospital"})


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
                    details         = encounter_details(record.document))

                mark_processed(db, record.id)

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
        "UWMC": "UWMedicalCenter", 
        "HMC": "Harborview", 
        "NWH":"Northwest",
        "UWNC": "UWNeighborhoodClinic",
        "SCH": "ChildrensHospitalSeattle"
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

    if sex_name not in sex_map:
        return "other"

    return sex_map[sex_name]

def encounter_details(document: dict) -> dict:
    """
    Describe encounter details in a simple data structure designed to be used
    from SQL.
    """
    return {
            "age": age(document.get("age")),      
            "collections": {
                "code": document.get("barcode"),
                "type": "RetrospectiveSample"
            }, 
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

def age(age: float) -> dict:
    """
    Given an *age*, return a dict containing its 'value' and a boolean for 
    'ninetyOrAbove'.

    Currently applys math.ceil() to age to match the age from Audere.
    This may change in the future as we push to report age in months for
    participants less than 1 year old.

    If no value is given for *age*, then will just retun None.
    """
    if age is None:
        return None

    return {
        "value": ceil(age),
        "ninetyOrAbove": ceil(age) >= 90
    }

def race(race_name: str) -> list:
    """
    Given a *race_name*, returns the matching race identifier found in Audere 
    survey data.
    """
    race_map = {
        "American Indian or Alaska Native": "americanIndianOrAlaskaNative",
        "Asian": "asian",
        "Black or African American": "blackOrAfricanAmerican",
        "Native Hawaiian or Other Pacific Islander": "nativeHawaiian",
        "White": "white",
        "Multiple races": "other",
        "Unknown": None, 
        "NULL": None
    }

    if race_name not in race_map:
        raise UnknownRaceError(f"Unknown race name «{race_name}»")
    
    return [race_map[race_name]]

def hispanic_latino(ethnic_group: str) -> list:
    """
    Given an *ethnic_group*, returns yes/no value for HispanicLatino key.
    """
    ethnic_map = {
        "Not Hispanic or Latino": "no",
        "Hispanic or Latino": "yes",
        "Unknown": None,
        "NULL": None
    }

    if ethnic_group not in ethnic_map:
        raise UnknownEthnicGroupError(f"Unknown ethnic group «{ethnic_group}»")
    
    return [ethnic_map[ethnic_group]]

def flu_shot(flu_shot_response: str) -> list:
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

def insurance(insurance_response: str) -> list:
    """
    Given an *insurance_response*, returns corresponding insurance
    identifier.
    """
    insurance_map = {
        "Commercial": "privateInsurance",
        "Medicaid": "government",
        "Medicare": "government",
        "Tricare": "government",
        "Other": "other"
    }
    if insurance_response not in insurance_map:
        return [None]
    return [insurance_map[insurance_response]]

def mark_processed(db, clinical_id: int) -> None:
    LOG.debug(f"Marking clinical document {clinical_id} as processed")

    data = {
        "clinical_id": clinical_id,
        "log_entry": Json({
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

class UnknownSiteError(ValueError):
    """
    Raised by :function:`site_identifier` if its provided *site_nickname*
    is not among the set of expected values.
    """
    pass

class UnknownRaceError(ValueError):
    """
    Raised by :function:`race` if its provided *race_name* is not among the set 
    of expected values.
    """
    pass

class UnknownEthnicGroupError(ValueError):
    """
    Raised by :function:`hispanic_latino` if its provided *ethnic_group* is not 
    among the set of expected values.
    """
    pass

class UnknownFluShotResponseError(ValueError):
    """
    Raised by :function:`flu_shot` if its provided *flu_shot_reponse* is not 
    among the set of expected values.
    """
    pass