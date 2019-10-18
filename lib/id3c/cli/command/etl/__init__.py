"""
Run ETL routines
"""
import click
import logging
from math import ceil
from typing import Any, Optional
from id3c.db.session import DatabaseSession
from id3c.db.datatypes import Json
from id3c.cli import cli
from id3c.db.types import MinimalSampleRecord


LOG = logging.getLogger(__name__)


@cli.group("etl", help = __doc__)
def etl():
    pass


# Load all ETL subcommands.
__all__ = [
    "enrollments",
    "manifest",
    "presence_absence",
    "clinical",
    "kit",
    "longitudinal",
    "consensus_genome",
    "redcap_det",
]

def find_or_create_site(db: DatabaseSession, identifier: str, details: dict) -> Any:
    """
    Select encounter site by *identifier*, or insert it if it doesn't exist.
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


def upsert_individual(db: DatabaseSession, identifier: str, sex: str = None) -> Any:
    """
    Upsert individual by their *identifier*.
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
                     age: Optional[str],
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
        "age": age,
        "details": Json(details),
    }

    encounter = db.fetch_row("""
        insert into warehouse.encounter (
                identifier,
                individual_id,
                site_id,
                encountered,
                age,
                details)
            values (
                %(identifier)s,
                %(individual_id)s,
                %(site_id)s,
                %(encountered)s::timestamp with time zone,
                %(age)s,
                %(details)s)

        on conflict (identifier) do update
            set individual_id = excluded.individual_id,
                site_id       = excluded.site_id,
                encountered   = excluded.encountered,
                age           = excluded.age,
                details       = excluded.details

        returning encounter_id as id, identifier
        """, data)

    assert encounter.id, "Upsert affected no rows!"

    LOG.info(f"Upserted encounter {encounter.id} «{encounter.identifier}»")

    return encounter


def find_sample_by_id(db: DatabaseSession, sample_id: int) -> Any:
    """
    Find sample by *sample_id* and return sample.
    """
    LOG.debug(f"Looking up sample «{sample_id}»")

    sample = db.fetch_row("""
        select sample_id as id, identifier, encounter_id
          from warehouse.sample
         where sample_id = %s
            for update
        """, (sample_id,))

    if not sample:
        LOG.error(f"No sample with id «{sample_id}» found")
        return None

    LOG.info(f"Found sample {sample.id} «{sample.identifier}»")
    return sample


def update_sample(db: DatabaseSession,
                  sample,
                  encounter_id: Optional[int]) -> Optional[MinimalSampleRecord]:
    """
    Update sample's encounter_id.
    """
    LOG.debug(f"Updating sample {sample.id}, linked to encounter {encounter_id}")

    if sample.encounter_id:
        assert sample.encounter_id == encounter_id, \
            f"Sample {sample.id} already linked to another encounter {sample.encounter_id}"
        return None

    sample = db.fetch_row("""
        update warehouse.sample
            set encounter_id = %s
        where sample_id = %s
        returning sample_id as id, identifier
        """, (encounter_id, sample.id))

    assert sample.id, "Updating encounter_id affected no rows!"

    LOG.info(f"Updated sample {sample.id}")

    return sample


def age(document: dict) -> Optional[str]:
    """
    Given a *document*, retrieve age value and
    return as a string to fit the interval format.

    If no value is given for age, then will just return None.
    """
    age = document.get("age")
    if age is None:
        return None
    return f"{float(age)} years"


def age_to_delete(age: Optional[Any]) -> Optional[dict]:
    """
    TODO: Delete this function once we remove age from details
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
        "value": min(ceil(float(age)), 90),
        "ninetyOrAbove": ceil(float(age)) >= 90
    }


def find_sample(db: DatabaseSession, identifier: str, for_update = True) -> Any:
    """
    Find sample by *identifier* and return sample.
    """
    LOG.debug(f"Looking up sample «{identifier}»")

    query_ending = ""

    if for_update:
        query_ending = "for update"

    sample = db.fetch_row("""
        select sample_id as id, identifier, encounter_id
          from warehouse.sample
         where identifier = %s or
               collection_identifier = %s
        """ + query_ending, (identifier,identifier,))

    if not sample:
        LOG.error(f"No sample with identifier «{identifier}» found")
        return None

    LOG.info(f"Found sample {sample.id} «{sample.identifier}»")
    return sample


def find_location(db: DatabaseSession, scale: str, identifier: str) -> Any:
    """
    Find a location by *scale* and *identifier*.
    """
    LOG.debug(f"Looking up location {(scale, identifier)}")

    location = db.fetch_row("""
        select location_id as id, scale, identifier, hierarchy
          from warehouse.location
         where (scale, identifier) = (%s, %s)
        """, (scale, identifier))

    if not location:
        LOG.error(f"No location for {(scale, identifier)}")
        return None

    LOG.info(f"Found location {location.id} as {(scale, identifier)}")
    return location


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
        "Native Hawaiian or other Pacific Islander": "nativeHawaiian",
        "nativehi": "nativeHawaiian",
        "White": "white",
        "Multiple races": "other",
        "Other": "other",
        "refused": None,
        "Prefer not to say": None,
    }

    def standardize_race(race):
        try:
            return race if race in race_map.values() else race_map[race]
        except KeyError:
            raise UnknownRaceError(f"Unknown race name «{race}»") from None

    return list(map(standardize_race, races))


def upsert_location(db: DatabaseSession,
                    scale: str,
                    identifier: str,
                    hierarchy: str) -> Any:
    """
    Upserts a location by its *scale* and *identifier*.

    If *hierarchy* is None and the location already exists, any existing
    hierarchy is preserved.
    """
    LOG.debug(f"Upserting location {(scale, identifier)}")

    location = db.fetch_row("""
        insert into warehouse.location (scale, identifier, hierarchy)
        values (%s, %s, %s)

        on conflict (scale, identifier) do update
            set hierarchy = coalesce(excluded.hierarchy, location.hierarchy)
                    || hstore(lower(location.scale), lower(location.identifier))

        returning location_id as id, scale, identifier, hierarchy
        """, (scale, identifier, hierarchy))

    assert location.id, "Upsert affected no rows!"

    LOG.info(f"Upserted location {location.id} as {(location.scale,location.identifier)}")

    return location


def upsert_encounter_location(db: DatabaseSession,
                              encounter_id: int,
                              relation: str,
                              location_id: int) -> Any:
    """
    Upserts an encounter location by its *encounter_id* and *relation*.
    """
    LOG.debug(f"Upserting encounter {relation} location")

    with db.cursor() as cursor:
        cursor.execute("""
            insert into warehouse.encounter_location (encounter_id, relation, location_id)
                values (%s, %s, %s)
                on conflict (encounter_id, relation) do update
                    set location_id = excluded.location_id
            """, (encounter_id, relation, location_id))

        assert cursor.rowcount == 1, "Upsert affected no rows!"


class SampleNotFoundError(ValueError):
    """
    Raised when a function is unable to find an existing sample with the given
    identifier.
    """
    pass

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


from . import *
