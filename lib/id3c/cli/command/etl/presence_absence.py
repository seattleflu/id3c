"""
Process presence-absence tests into the relational warehouse

The ETL process performs a `find_or_create` for targets, as we anticipate the same targets to be tested, but leave room for the addition of new targets.

The ETL process performs a `find_or_create` for samples to allow the processing of re-tested samples. We do not expect to update information of a sample itself.

The ETL process performs an `upsert` for presence_absence to allow the update of results should there be a re-test on an old sample.

The presence-absence ETL process will abort under these conditions:

1. If a sample's barcode matches with a UUID that is of the incorrect identifier set

2. If we receive an unexpected value for the "controlStatus" of a target

3. If we receive an unexpected value for the "targetResult" of a specific test
"""
import click
import logging
from datetime import datetime, timezone
from typing import Any, Optional
from id3c.db import find_identifier
from id3c.db.session import DatabaseSession
from id3c.db.datatypes import Json
from . import etl, SampleNotFoundError


LOG = logging.getLogger(__name__)


# This revision number is stored in the processing_log of each presence-absence
# record when the presence-absence test is successfully processed by this ETL
# routine. The routine finds new-to-it records to process by looking for
# presence-absence tests lacking this revision number in their log.  If a
# change to the ETL routine necessitates re-processing all presence-absence tests,
# this revision number should be incremented.
REVISION = 4


@etl.command("presence-absence", help = __doc__)

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

def etl_presence_absence(*, action: str):
    LOG.debug(f"Starting the presence_absence ETL routine, revision {REVISION}")

    db = DatabaseSession()

    # Fetch and iterate over presence-absence tests that aren't processed
    #
    # Rows we fetch are locked for update so that two instances of this
    # command don't try to process the same presence-absence tests.
    LOG.debug("Fetching unprocessed presence-absence tests")

    presence_absence = db.cursor("presence_absence")
    presence_absence.execute("""
        select presence_absence_id as id, document
          from receiving.presence_absence
         where not processing_log @> %s
         order by id
           for update
        """, (Json([{ "revision": REVISION }]),))

    processed_without_error = None

    try:
        for group in presence_absence:
            with db.savepoint(f"presence_absence group {group.id}"):
                LOG.info(f"Processing presence_absence group {group.id}")

                # Samplify will now send documents with a top level key
                # "samples". The new format also includes a "chip" key for each
                # sample which is then included in the unique identifier for
                # each presence/absence result
                #   -Jover, 14 Nov 2019
                try:
                    received_samples = group.document["samples"]
                except KeyError as error:
                    # Skip documents in the old format because they do not
                    # include the "chip" key which is needed for the
                    # unique identifier for each result.
                    #   -Jover, 14 Nov 2019
                    if (group.document.get("store") is not None or
                        group.document.get("Update") is not None):

                        LOG.info("Skipping presence_absence record that is in old format")
                        mark_processed(db, group.id)
                        continue

                    else:
                        raise error from None

                for received_sample in received_samples:
                    received_sample_barcode = received_sample["investigatorId"]
                    LOG.info(f"Processing sample «{received_sample_barcode}»")

                    received_sample_identifier = sample_identifier(db, received_sample_barcode)

                    if not received_sample_identifier:
                        LOG.warning(f"Skipping results for sample without a known identifier «{received_sample_barcode}»")
                        continue

                    sample = update_sample(db,
                        identifier = received_sample_identifier,
                        additional_details = sample_details(received_sample))

                    received_sample_id = str(received_sample["sampleId"])
                    chip = received_sample["chip"]

                    # Guard against empty chip values
                    assert chip, "Received bogus chip id"

                    for test_result in received_sample["targetResults"]:
                        test_result_target_id = test_result["geneTarget"]
                        LOG.debug(f"Processing target «{test_result_target_id}» for \
                        sample «{received_sample_barcode}»")

                        # Most of the time we expect to see existing targets so a
                        # select-first approach makes the most sense to avoid useless
                        # updates.
                        target = find_or_create_target(db,
                            identifier = test_result_target_id,
                            control = target_control(test_result["controlStatus"]))

                        # Guard against bad data pushes we've seen from NWGC.
                        # This isn't great, but it's better than processing the
                        # data as-sent.
                        assert test_result["id"] > 0, "bogus targetResult id"

                        # Old format for the presence/absence identifier prior
                        # to the format change
                        #  -Jover, 07 November 2019
                        old_identifier = f"NWGC_{test_result['id']}"

                        # With the new format, the unqiue identifier for each
                        # result is NWGC/{sampleId}/{geneTarget}/{chip}
                        new_identifier = "/".join([
                            "NWGC",
                            received_sample_id,
                            test_result_target_id,
                            chip
                        ])

                        # Update all old format identifiers to the new format
                        # so that the following upsert can work as expected
                        update_presence_absence_identifier(db,
                            new_identifier = new_identifier,
                            old_identifier = old_identifier
                        )

                        # Most of the time we expect to see new samples and new
                        # presence_absence tests, so an insert-first approach makes more sense.
                        # Presence-absence tests we see more than once are presumed to be
                        # corrections.
                        upsert_presence_absence(db,
                            identifier = new_identifier,
                            sample_id  = sample.id,
                            target_id  = target.id,
                            present    = get_target_result(test_result["targetStatus"]),
                            details = presence_absence_details(test_result))

                mark_processed(db, group.id)

                LOG.info(f"Finished processing presence_absence group {group.id}")

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
                "Commit successfully processed presence-absence tests up to this point?"

            commit = click.confirm(ask_to_commit)
        else:
            commit = action == "commit"

        if commit:
            LOG.info(
                "Committing all changes" if processed_without_error else \
                "Committing successfully processed presence-absence tests up to this point")
            db.commit()

        else:
            LOG.info("Rolling back all changes; the database will not be modified")
            db.rollback()

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


def target_control(control: str) -> bool:
    """
    Determine the control status of the target.
    """
    expected_values = ["NotControl", "PositiveControl"]
    if not control or control not in expected_values:
        raise UnknownControlStatusError(f"Unknown control status «{control}».")
    return control == "PositiveControl"


def update_sample(db: DatabaseSession,
                  identifier: str,
                  additional_details: dict) -> Any:
    """
    Find sample by *identifier* and update with any *additional_details*.

    The provided *additional_details* are merged (at the top-level only) into
    the existing sample details, if any.

    Raises an :class:`SampleNotFoundError` if there is no sample known by
    *identifier*.
    """
    LOG.debug(f"Looking up sample «{identifier}»")

    sample = db.fetch_row("""
        select sample_id as id, identifier
          from warehouse.sample
         where identifier = %s
           for update
        """, (identifier,))

    if not sample:
        LOG.error(f"No sample with identifier «{identifier}» found")
        raise SampleNotFoundError(identifier)

    LOG.info(f"Found sample {sample.id} «{sample.identifier}»")

    if additional_details:
        LOG.info(f"Updating sample {sample.id} «{sample.identifier}» details")

        sample = db.fetch_row("""
            update warehouse.sample
               set details = coalesce(details, '{}') || %s
             where sample_id = %s
            returning sample_id as id, identifier
            """, (Json(additional_details), sample.id))

        assert sample.id, "Updating details affected no rows!"

    return sample


def sample_identifier(db: DatabaseSession, barcode: str) -> Optional[str]:
    """
    Find corresponding UUID for scanned sample barcode within
    warehouse.identifier.
    """
    identifier = find_identifier(db, barcode)

    if identifier:
        assert identifier.set_name == "samples", \
            f"Identifier found in set «{identifier.set_name}», not «samples»"

    return identifier.uuid if identifier else None


def sample_details(document: dict) -> dict:
    """
    Capture NWGC sample ID.
    Capture details about the go/no-go sequencing call for this sample.
    """
    return {
        "nwgc_id": document['sampleId'],
        "sequencing_call": {
            "comment": document['sampleComment'],
            "initial": document['initialProceedToSequencingCall'],
            "final": document["sampleProceedToSequencing"],
        },
    }

def presence_absence_details(document: dict) -> dict:
    """
    Describe presence/absence details in a simple data structure designed to
    be used from SQL.
    """
    return {
        "replicates": document['wellResults']
    }


def update_presence_absence_identifier(db: DatabaseSession,
                                       old_identifier: str,
                                       new_identifier: str) -> None:
    """
    Update the presence absence identifier if the presence absence result
    already exists within warehouse.presence_absence with the *old_identifier*
    """
    LOG.debug(f"Updating presence_absence identifier «{old_identifier}» with new identifier «{new_identifier}»")

    identifiers = {
        "old_identifier": old_identifier,
        "new_identifier": new_identifier
    }

    with db.cursor() as cursor:
        cursor.execute("""
            update warehouse.presence_absence
              set identifier = %(new_identifier)s
             where identifier = %(old_identifier)s
        """, identifiers)


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
                details = excluded.details

        returning presence_absence_id as id, identifier
        """, data)

    assert presence_absence.id, "Upsert affected no rows!"

    LOG.info(f"Upserted presence_absence {presence_absence.id} \
        «{presence_absence.identifier}»")

    return presence_absence


def get_target_result(target_status: str) -> Any:
    """
    Takes a given target status and its sample and target ids. Returns the decoded
    target_result as a boolean if the given target status is known. If the given
    target status is an unexpected value, error will be raised and the ETL process will abort.
    """
    expected_values = ['Detected', 'NotDetected']

    if not target_status or target_status not in expected_values:
        raise UnknownTargetResultError(f"Unknown target result «{target_status}».")

    return target_status == 'Detected'


def mark_processed(db, group_id: int) -> None:
    LOG.debug(f"Marking presence_absence group {group_id} as processed")

    data = {
        "group_id": group_id,
        "log_entry": Json({
            "revision": REVISION,
            "timestamp": datetime.now(timezone.utc),
        }),
    }

    with db.cursor() as cursor:
        cursor.execute("""
            update receiving.presence_absence
               set processing_log = processing_log || %(log_entry)s
             where presence_absence_id = %(group_id)s
            """, data)


class UnknownControlStatusError(ValueError):
    """
    Raised by :function:`target_control` if its provided *control*
    is not among the set of expected values.
    """
    pass

class UnknownTargetResultError(ValueError):
    """
    Raised by :function:`get_target_result` if its provided *target_result*
    is not among the set of expected values.
    """
    pass
