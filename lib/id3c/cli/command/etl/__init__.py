"""
Run ETL routines
"""
import click
import logging
from math import ceil
from typing import Any, Dict, Optional
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
    "kit",
    "consensus_genome",
    "redcap_det",
    "fhir",
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


def upsert_individual(db: DatabaseSession, identifier: str, sex: str = None, details: dict = None) -> Any:
    """
    Upsert individual by their *identifier*.
    """

    # This will only perform a shallow merge of the details for an existing record.
    # To make it more robust, this should be updated to a deep merge, using identifiers
    # nested within the FHIR resource entries to append only new information.
    # -drr, 7 Jan 2022

    LOG.debug(f"Upserting individual «{identifier}»")

    data = {
        "identifier": identifier,
        "sex": sex,
        "details": Json(details),
    }

    individual = db.fetch_row("""
        insert into warehouse.individual (identifier, sex, details)
            values (%(identifier)s, %(sex)s, %(details)s)

        on conflict (identifier) do update
            set sex     = excluded.sex,
                details = coalesce(individual.details, '{}'::jsonb) || excluded.details

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

    # Select on identifier to determine if the encounter record already exists.
    #
    # This query also compares the existing vs. incoming row to determine if an update
    # is necessary. If a record is found with the given identifier, the columns that would be
    # set on update are compared to the incoming values, ignoring any values in details starting
    # with `urn:uuid:` (FHIR resource reference IDs that are randomly generated each time a FHIR
    # bundle is constructed, and are not indicitive of data having changed).
    #
    # Note: if there are schema changes to `warehouse.encounter` resulting in changes to the columns
    # that are set on update, the list of columns being compared should be updated to match.

    with db.cursor() as cursor:
        cursor.execute("""
            select encounter_id as id,
                identifier,
                (
                    row (encounter.individual_id,
                        encounter.site_id,
                        encounter.encountered,
                        encounter.age,
                        regexp_replace(encounter.details::text, '"urn:uuid:[a-f0-9-]{36}"', '""', 'g'))
                    !=
                    row (%(individual_id)s::integer,
                            %(site_id)s::integer,
                            %(encountered)s::timestamp with time zone,
                            %(age)s::interval,
                            regexp_replace((%(details)s::jsonb)::text, '"urn:uuid:[a-f0-9-]{36}"', '""', 'g'))
                ) as data_changed
            from warehouse.encounter
            where identifier = %(identifier)s
            for update
            """, data)

        encounters = list(cursor)

    # Nothing found → create
    if not encounters:
        LOG.info("Creating new encounter")

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
        returning encounter_id as id, identifier
        """, data)

    # One found → update
    elif len(encounters) == 1:
        encounter = encounters[0]

        LOG.info(f"Updating existing encounter {encounter.id}")

        if encounter.data_changed==False:
            LOG.info(f"Skipping upsert for encounter {encounter.id} «{identifier}» (no change).")
            return encounter

        encounter = db.fetch_row("""
            update warehouse.encounter
                set individual_id = %(individual_id)s,
                    site_id = %(site_id)s,
                    encountered = %(encountered)s,
                    age = %(age)s,
                    details = %(details)s
            where encounter_id = %(encounter_id)s
                returning encounter_id as id, identifier
        """, { **data, "encounter_id": encounter.id })

    # More than one found → error
    else:
        raise Exception(f"More than one encounter matching identifier: {identifier}")

    if encounter:
        LOG.info(f"Upserted encounter {encounter.id} «{identifier}»")

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
                  encounter_id: Optional[int]=None) -> Optional[MinimalSampleRecord]:
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
        LOG.info(f"No sample with identifier «{identifier}» found")
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


def upsert_location(db: DatabaseSession,
                    scale: str,
                    identifier: str,
                    hierarchy: str) -> Any:
    """
    Upserts a location by its *scale* and *identifier*.

    If *hierarchy* is None, it will be set to the location's
    `scale => identifier`. Otherwise, the location's `scale => identifier`
    will be appended to the *hierarchy*.

    On update, new hierarchy and existing hierarchy are concatenated, with
    new hierarchy taking precedence if there is overlap of keys.
    """
    LOG.debug(f"Upserting location {(scale, identifier)}")

    # Always includes the new location's own scale => identifier in hierarchy
    location_hierarchy = f"{scale} => {identifier}".lower()
    if hierarchy is None:
        hierarchy = location_hierarchy
    else:
        hierarchy = hierarchy + "," + location_hierarchy

    location = db.fetch_row("""
        insert into warehouse.location (scale, identifier, hierarchy)
        values (%s, %s, %s)

        on conflict (scale, identifier) do update
            set hierarchy = coalesce(location.hierarchy, '') || excluded.hierarchy

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
    LOG.debug(f"Upserting encounter with location '{relation}'")

    data = {
        "encounter_id": encounter_id,
        "relation": relation,
        "location_id": location_id,
    }

    with db.cursor() as cursor:
        cursor.execute("""
            select
                encounter_id,
                relation,
                encounter_location.location_id != %(location_id)s::integer
                    as data_changed
            from warehouse.encounter_location
            where
                encounter_id = %(encounter_id)s and
                relation = %(relation)s
            for update
            """, data
        )

        encounter_locs = list(cursor)

    if not encounter_locs:
        LOG.info("Adding new encounter location")

        encounter_loc = db.fetch_row("""
            insert into warehouse.encounter_location(
                    encounter_id,
                    relation,
                    location_id
                )
                values (
                    %(encounter_id)s,
                    %(relation)s,
                    %(location_id)s
                )
            returning encounter_id, relation
            """, data
        )

    elif len(encounter_locs) == 1:
        encounter_loc = encounter_locs[0]

        LOG.info("Updating existing encounter location")

        if encounter_loc.data_changed == False:
            LOG.info(f"Skipping upsert for encounter with location '{relation}' (no change).")
            return

        encounter_loc = db.fetch_row("""
            update warehouse.encounter_location
                set
                    location_id = %(location_id)s
            where
                encounter_id = %(encounter_id)s and
                relation = %(relation)s
            returning encounter_id, relation
            """, data
        )

    else:
        raise Exception(f"More than one encounter matching. encounter_id: {encounter_id}, relation: {relation}")

    if encounter_loc:
        LOG.info(f"Upserted encounter with location '{relation}'")

    assert encounter_loc, "Upsert affected no rows!"


def upsert_presence_absence(db: DatabaseSession,
                            identifier: str,
                            sample_id: int,
                            target_id: int,
                            present: bool,
                            details: dict) -> Any:
    """
    Upsert presence_absence by its *identifier*.

    Confirmed with Samplify that their numeric identifier for each test is stable
    and persistent.
    """
    LOG.debug(f"Upserting presence_absence «{identifier}»")

    data = {
        "identifier": identifier,
        "sample_id": sample_id,
        "target_id": target_id,
        "present": present,
        "details": Json(details)
    }

    presence_absence = db.fetch_row("""
        insert into warehouse.presence_absence (
                identifier,
                sample_id,
                target_id,
                present,
                details)
            values (
                %(identifier)s,
                %(sample_id)s,
                %(target_id)s,
                %(present)s,
                %(details)s)

        on conflict (identifier) do update
            set sample_id = excluded.sample_id,
                target_id = excluded.target_id,
                present   = excluded.present,
                details = coalesce(presence_absence.details, '{}') || excluded.details

        returning presence_absence_id as id, identifier
        """, data)

    assert presence_absence.id, "Upsert affected no rows!"

    LOG.info(f"Upserted presence_absence {presence_absence.id} \
        «{presence_absence.identifier}»")

    return presence_absence


def find_or_create_target(db: DatabaseSession, identifier: str, control: bool) -> Any:
    """
    Select presence_absence test target by *identifier*, or insert it if it doesn't exist.
    """
    LOG.debug(f"Looking up target «{identifier}»")

    target = db.fetch_row("""
        select target_id as id, identifier
          from warehouse.target
         where identifier = %s
        """, (identifier,))

    if target:
        LOG.info(f"Found target {target.id} «{target.identifier}»")
    else:
        LOG.debug(f"Target «{identifier}» not found, adding")

        data = {
            "identifier": identifier,
            "control": control
        }

        target = db.fetch_row("""
            insert into warehouse.target (identifier, control)
                values (%(identifier)s, %(control)s)
            returning target_id as id, identifier
            """, data)

        LOG.info(f"Created target {target.id} «{target.identifier}»")

    return target


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

class UnknownCovidScreenError(ValueError):
    """
    Raised by :function:`covid_screen` if its provided *is_covid_screen* is not
    among the set of expected values.
    """
    pass

class UnknownCovidShotResponseError(ValueError):
    """
    Raised by :function:`covid_shot` if its provided *covid_shot_response* is not
    among the set of expected values.
    """
    pass

class UnknownCovidShotManufacturerError(ValueError):
    """
    Raised by :function:`covid_shot_manufacturer` if its provided *covid_shot_manufacturer_name* is not
    among the set of expected values.
    """
    pass

class UnknownAdmitEncounterResponseError(ValueError):
    """
    Raised by :function:`admit_encounter` if its provided *admit_encounter_response* is not
    among the set of expected values.
    """
    pass

class UnknownAdmitICUResponseError(ValueError):
    """
    Raised by :function:`admit_icu` if its provided *admit_icu_response* is not
    among the set of expected values.
    """
    pass

from . import *
