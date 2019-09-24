"""
Process both enrollment and manifest documents to extract kit related data
into the relational warehouse.

The kit etl is completely dependent on the enrollments etl and the manifest etl.
It does not create encounters or samples, but must find them within the
relational warehouse.
"""
import click
import logging
from psycopg2 import sql
from datetime import datetime, timezone
from typing import Any, Optional, Tuple
from seattleflu.db import find_identifier
from seattleflu.db.session import DatabaseSession
from seattleflu.db.datatypes import Json
from seattleflu.db.types import KitRecord, SampleRecord
from . import etl, update_sample, find_sample_by_id

LOG = logging.getLogger(__name__)


# The revision number and etl name are stored in the processing_log of each
# enrollment/manifest record when the record is processed or skipped by
# this ETL routine. The routine finds new-to-it records to process by looking
# for records lacking this etl revision number and etl name in their log.
# If a change to the ETL routine necessitates re-processing all enrollments,
# this revision number should be incremented.
# The etl name has been added to allow multiple etls to process the same
# receiving table.
ENROLLMENTS_REVISION = 1
MANIFEST_REVISION = 1
ETL_NAME = "kit"

expected_identifier_sets = {
    "kits": {"kits-fluathome.org"},
    "samples": {"samples", "collections-fluathome.org"},
    "test-strips": {"test-strips-fluathome.org"}
}

@etl.group("kit", help = __doc__)
def kits():
    pass

@kits.command("enrollments", help = __doc__)

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

def kit_enrollments(*, action: str):
    LOG.debug(f"Starting the kit enrollments ETL routine, revision {ENROLLMENTS_REVISION}")

    db = DatabaseSession()

    expected_barcode_types = {"ScannedSelfSwab", "ManualSelfSwab"}

    LOG.debug("Fetching unprocessed enrollments")
    enrollments = db.cursor("enrollments")
    enrollments.execute("""
        select enrollment_id as id, document
          from receiving.enrollment
         where not processing_log @> %s
         order by id
          for update
        """, (Json([{ "etl": ETL_NAME, "revision": ENROLLMENTS_REVISION }]),))

    processed_without_error = None

    try:
        for enrollment in enrollments:
            with db.savepoint(f"enrollment {enrollment.id}"):
                LOG.info(f"Processing enrollment {enrollment.id}")

                # Find encounter that should have been created
                # from this enrollment record through etl enrollments
                encounter = find_encounter(db, enrollment.document["id"])

                # Error out the kit etl process if no encounter found
                # The kit etl process can try again starting with this record
                # next time with the idea that the encounter will be
                # created by then.
                if not encounter:
                    raise EncounterNotFoundError(f"No encounter with identifier «{enrollment.document['id']}» found")

                # Skip and mark the enrollment document as processed if the
                # encounter found is linked to a site that is not self-test
                if encounter.site != "self-test":
                    LOG.debug(f"Found encounter {encounter.id} «{encounter.identifier}»" +
                              f"linked to site «{encounter.site}», not 'self-test'")
                    mark_enrollment_processed(db, enrollment.id)
                    continue

                for code in enrollment.document["sampleCodes"]:
                    barcode = code.get("code")

                    # Kit must have a barcode
                    if not barcode:
                        LOG.warning(f"No barcode found in sampleCodes {code}")
                        continue

                    # Barcode must be of expected barcode type
                    if code["type"] not in expected_barcode_types:
                        LOG.debug(f"Skipping barcode with type {code['type']}")
                        continue

                    # Convert kit barcode to full identifier
                    kit_identifier = find_identifier(db, barcode)

                    if not kit_identifier:
                        LOG.warning(f"Skipping kit with unknown barcode «{barcode}»")
                        continue

                    if kit_identifier.set_name not in expected_identifier_sets["kits"]:
                        LOG.warning(f"Skipping kit with identifier found in " +
                                    f"set «{kit_identifier.set_name}» not {expected_identifier_sets['kits']}")
                        continue

                    details = {
                        "type": code["type"]
                    }

                    kit, status = upsert_kit_with_encounter(db,
                            identifier          = kit_identifier.uuid,
                            encounter_id        = encounter.id,
                            additional_details  = details)

                    if status == "updated":
                        update_kit_samples(db, kit)

                mark_enrollment_processed(db, enrollment.id)

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


def find_encounter(db: DatabaseSession, identifier: str) -> Any:
    """
    Given an *identifier* find the corresponding encounter within the
    relational warehouse.
    """
    LOG.debug(f"Looking up encounter with identifier «{identifier}»")

    encounter = db.fetch_row("""
        select encounter_id as id,
               encounter.identifier as identifier,
               site.identifier as site
          from warehouse.encounter
          join warehouse.site using (site_id)
         where encounter.identifier = %s
        """, (identifier,))

    if not encounter:
        return None

    LOG.info(f"Found encounter «{encounter.id}»")
    return encounter


def upsert_kit_with_encounter(db: DatabaseSession,
                              identifier: str,
                              encounter_id: int,
                              additional_details: dict) -> Tuple[KitRecord, str]:
    """
    Upsert kit by its *identifier* to include link to encounter.

    An existing kit has its *encounter_id* updated and the
    provided *additional_details* are merged (at the top level only into)
    the existing kit details, if any.
    """
    LOG.debug(f"Upserting kit «{identifier}»")

    data = {
        "identifier": identifier,
        "encounter_id": encounter_id,
        "additional_details": Json(additional_details)
    }

    # Look for existing kit
    kit = find_kit(db, identifier)

    # Nothing found → create
    if not kit:
        LOG.info("Creating new kit")
        status = "created"
        kit = db.fetch_row("""
            insert into warehouse.kit (identifier, encounter_id, details)
                values(%(identifier)s,
                       %(encounter_id)s,
                       %(additional_details)s)
            returning kit_id as id, identifier, encounter_id, null rdt_sample_id, null utm_sample_id
            """, data)

    # Found kit → update
    else:
        status = "updated"
        # Warn if kit is already linked to a different encounter!
        if kit.encounter_id and kit.encounter_id != encounter_id:
            LOG.warning(f"Kit «{kit.id}» already linked to another encounter «{kit.encounter_id}»")

        kit = db.fetch_row("""
            update warehouse.kit
               set encounter_id = %(encounter_id)s,
                   details = coalesce(details, '{}') || %(additional_details)s

             where kit_id = %(kit_id)s

            returning kit_id as id, identifier, encounter_id, rdt_sample_id, utm_sample_id
            """, { **data, "kit_id": kit.id })

    assert kit.id, "Upsert affected no rows!"

    LOG.info(f"Upserted kit {kit.id} with identifier «{kit.identifier}» linked to encounter «{kit.encounter_id}»")

    return kit, status


def mark_enrollment_processed(db, enrollment_id: int) -> None:
    LOG.debug(f"Marking enrollment {enrollment_id} as processed")

    data = {
        "enrollment_id": enrollment_id,
        "log_entry": Json({
            "revision": ENROLLMENTS_REVISION,
            "etl": ETL_NAME,
            "timestamp": datetime.now(timezone.utc),
        })
    }

    with db.cursor() as cursor:
        cursor.execute("""
            update receiving.enrollment
               set processing_log = processing_log || %(log_entry)s
             where enrollment_id = %(enrollment_id)s
            """, data)

@kits.command("manifest", help = __doc__)

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

def kit_manifests(*, action: str):
    LOG.debug(f"Starting the kits manifests ETL routine, revision {MANIFEST_REVISION}")

    db = DatabaseSession()

    LOG.debug("Fetching unprocessed manifest records")

    manifest = db.cursor("manifest")
    manifest.execute("""
        select manifest_id as id, document
          from receiving.manifest
         where not processing_log @> %s
         order by id
           for update
        """, (Json([{ "etl": ETL_NAME, "revision": MANIFEST_REVISION }]),))

    processed_without_error = None

    try:
        for manifest_record in manifest:
            with db.savepoint(f"manifest record {manifest_record.id}"):
                LOG.info(f"Processing record {manifest_record.id}")

                # Mark record as skipped
                # if it does not contain a kit related sample
                if "kit" not in manifest_record.document:
                    LOG.info(f"Skipping manifest record {manifest_record.id} without kit data")
                    mark_skipped(db, manifest_record.id)
                    continue

                sample_barcode = manifest_record.document.pop("sample")
                sample_identifier = find_identifier(db, sample_barcode)

                # Mark record as skipped
                # if it has an unknown sample barcode
                if not sample_identifier:
                    LOG.warning(f"Skipping manifest record with unknown sample barcode «{sample_barcode}»")
                    mark_skipped(db, manifest_record.id)
                    continue

                # Mark record as skipped sample identifier set is unexpected
                if sample_identifier.set_name not in expected_identifier_sets["samples"]:
                    LOG.warning(f"Skipping manifest record with sample identifier found in " +
                                f"set «{sample_identifier.set_name}», not {expected_identifier_sets['samples']}")
                    mark_skipped(db, manifest_record.id)
                    continue

                # Find sample that should have been created from this
                # manifest record via etl manifest
                sample = find_sample(db, sample_identifier.uuid)

                # Error out the kit etl process if no sample found
                # The kit etl process can try again starting with this record
                # next time with the idea that the sample will be
                # created by then.
                if not sample:
                    raise SampleNotFoundError(f"No sample with «{sample_identifier.uuid}» found")

                # Mark record as skipped if the sample does not have a
                # sample type (utm or rdt)
                if sample.type not in {"utm", "rdt"}:
                    LOG.info(f"Skipping manifest record {manifest_record.id} "+
                             f"with unknown sample type {sample.type}")
                    mark_skipped(db, manifest_record.id)
                    continue

                kit_barcode = manifest_record.document.pop("kit")
                kit_identifier = find_identifier(db, kit_barcode)

                # Mark record as skipped if it has an unknown kit barcode
                if not kit_identifier:
                    LOG.warning(f"Skipping kit with unknown barcode «{kit_barcode}»")
                    mark_skipped(db, manifest_record.id)
                    continue

                # Mark record as skipped if kit identifier set is unexpected
                if kit_identifier.set_name not in expected_identifier_sets["kits"]:
                    LOG.warning(f"Skipping kit with identifier found in " +
                                f"set «{kit_identifier.set_name}» not {expected_identifier_sets['kits']}")
                    mark_skipped(db, manifest_record.id)
                    continue

                # List of extra data not needed for kit record that can
                # be removed before adding manifest document to kit details
                extra_data = ["collection", "sample_type",
                              "aliquot_date", "aliquots", "racks"]
                for key in extra_data:
                    manifest_record.document.pop(key, None)

                # Try to find identifier for the test-strip barcode for rdt samples
                if sample.type == "rdt":
                    update_test_strip(db, manifest_record.document)

                kit, status = upsert_kit_with_sample(db,
                    identifier          = kit_identifier.uuid,
                    sample              = sample,
                    additional_details  = manifest_record.document)

                if status == "updated":
                    update_sample(db, sample, kit.encounter_id)

                mark_loaded(db, manifest_record.id, status, kit.id)

    except Exception as error:
        processed_without_error = False

        LOG.error(f"Aborting with error: {error}")
        raise error from None

    else:
        processed_without_error = True

    finally:
        if action == "prompt":
            ask_to_commit = \
                "Commit all changes?" if processed_without_error else \
                "Commit successfully processed manifest records up to this point?"

            commit = click.confirm(ask_to_commit)
        else:
            commit = action == "commit"

        if commit:
            LOG.info(
                "Committing all changes" if processed_without_error else \
                "Committing successfully processed manifest records up to this point")
            db.commit()

        else:
            LOG.info("Rolling back all changes; the database will not be modified")
            db.rollback()


def find_sample(db: DatabaseSession, identifier: str) -> Optional[SampleRecord]:
    """
    Given an *identifier* find the corresponding sample within the
    database.
    """
    LOG.debug(f"Looking up sample with identifier «{identifier}»")

    sample = db.fetch_row("""
        select sample_id as id,
               identifier,
               encounter_id,
               details ->> 'sample_type' as type
           from warehouse.sample
          where sample.identifier = %s
        """, (identifier,))

    if not sample:
        return None

    LOG.info(f"Found sample «{sample.id}»")
    return sample


def update_test_strip(db: DatabaseSession, document: dict):
    """
    Find identifier that matches the test_strip barcode within *document*.
    Updates *document* to have both the test_strip barcode and
    the identifier if found.
    """
    strip_barcode = document["test_strip"]
    strip_identifier = find_identifier(db, strip_barcode)

    document["test_strip"] = {
        "uuid": None,
        "barcode": strip_barcode
    }

    if not strip_identifier:
        LOG.warning(f"Test strip has unknown barcode «{strip_barcode}»")

    elif strip_identifier.set_name not in expected_identifier_sets["test-strips"]:
        LOG.warning(f"Test strip barcode found in unexpected identifier set «{strip_identifier.set_name}»")

    else:
        document["test_strip"] = {
            "uuid": strip_identifier.uuid,
            "barcode": strip_identifier.barcode
        }


def upsert_kit_with_sample(db: DatabaseSession,
                           identifier: str,
                           sample: SampleRecord,
                           additional_details: dict) -> Tuple[KitRecord, str]:
    """
    Upsert kit by its *identifier* to include link to a sample.

    An existing kit has its *sample_id* updated and the provided
    *additional_details* are merged (at the top level only into)
    the existing kit details, if any.
    """
    LOG.debug(f"Upserting kit «{identifier}»")

    data = {
        "identifier": identifier,
        "sample_id": sample.id,
        "additional_details": Json(additional_details)
    }

    if sample.type == 'utm':
        sample_type = "utm_sample_id"
    elif sample.type == 'rdt':
        sample_type = "rdt_sample_id"

    # Look for existing kit
    kit = find_kit(db, identifier)

    # Nothing found → create
    if not kit:
        LOG.info("Creating new kit")
        status = "created"
        kit = db.fetch_row(sql.SQL("""
            insert into warehouse.kit (identifier, {}, details)
                values(%(identifier)s,
                       %(sample_id)s,
                       %(additional_details)s)
            returning kit_id as id,
                      identifier,
                      encounter_id,
                      {}
            """).format(sql.Identifier(sample_type),
                        sql.Identifier(sample_type)), data)

    # Found kit → update
    else:
        status = "updated"
        kit_sample_id = getattr(kit, sample_type)
        # Warn if kit is already linked to a different sample!
        if (kit_sample_id and (sample.id != kit_sample_id)):
            LOG.warning(f"Kit «{kit.id}» already linked to another " +
                        f"{sample_type} «{kit_sample_id}»")

        kit = db.fetch_row(sql.SQL("""
            update warehouse.kit
               set {} = %(sample_id)s,
                   details = coalesce(details, {}) || %(additional_details)s

             where kit_id = %(kit_id)s

            returning kit_id as id,
                      identifier,
                      encounter_id,
                      {}
            """).format(sql.Identifier(sample_type),
                        sql.Literal(Json({})),
                        sql.Identifier(sample_type)),
                        { **data, "kit_id": kit.id })

    assert kit.id, "Upsert affected no rows!"

    LOG.info(f"Upserted kit {kit.id} with identifier «{kit.identifier}» " +
             f"linked to {sample_type} «{getattr(kit, sample_type)}»")

    return kit, status


def mark_loaded(db, manifest_id: int, status: str, kit_id: int) -> None:
    LOG.debug(f"Marking kit sample manifest record {manifest_id} as loaded")
    mark_manifest_processed(db, manifest_id, { "status": status, "kit_id": kit_id })


def mark_skipped(db, manifest_id: int) -> None:
    LOG.debug(f"Marking sample manifest record {manifest_id} as skipped")
    mark_manifest_processed(db, manifest_id, { "status": "skipped" })


def mark_manifest_processed(db, manifest_id: int, entry = {}) -> None:
    LOG.debug(f"Marking manifest {manifest_id} as processed")

    data = {
        "manifest_id": manifest_id,
        "log_entry": Json({
            **entry,
            "revision": MANIFEST_REVISION,
            "etl": ETL_NAME,
            "timestamp": datetime.now(timezone.utc)
        })
    }

    with db.cursor() as cursor:
        cursor.execute("""
            update receiving.manifest
               set processing_log = processing_log || %(log_entry)s
             where manifest_id = %(manifest_id)s
            """, data)


def find_kit(db: DatabaseSession, identifier: str) -> KitRecord:
    """
    Look for kit using *identifier* within the database
    """
    kit: KitRecord = db.fetch_row("""
        select kit_id as id, identifier, encounter_id, rdt_sample_id, utm_sample_id
          from warehouse.kit
         where identifier = %s
            for update
        """, (identifier,))

    return kit


def update_kit_samples(db: DatabaseSession, kit: KitRecord):
    """
    After upserting kit, update the samples linked to the kit.
    """
    if kit.rdt_sample_id:

        rdt_sample = find_sample_by_id(db, kit.rdt_sample_id)

        if rdt_sample:
            update_sample(db,
                sample         = rdt_sample,
                encounter_id   = kit.encounter_id)

    if kit.utm_sample_id:

        utm_sample = find_sample_by_id(db, kit.utm_sample_id)

        if utm_sample:

            update_sample(db,
                sample         = utm_sample,
                encounter_id   = kit.encounter_id)


class EncounterNotFoundError(ValueError):
    """
    Raised by the kit enrollments etl if it cannot find an encounter within
    the relational warehouse using the provided *identifier*
    """
    pass


class SampleNotFoundError(ValueError):
    """
    Raised by the kit manifest etl if it cannot find a sample within
    the relational warehouse using the provided *identifier*
    """
    pass
