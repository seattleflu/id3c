"""
Process kit results into the relational warehouse

The kit-result etl is completely depending on the kit etl and the manifest etl.
It does not create kits or samples, but must find them within the relational
warehouse.
"""
import click
import logging
from datetime import datetime, timezone
from typing import Any
from seattleflu.db.session import DatabaseSession
from seattleflu.db.datatypes import Json
from seattleflu.db import find_identifier
from . import etl, find_or_create_target, upsert_presence_absence


LOG = logging.getLogger(__name__)

# This revision number is stored in the processing_log of each kit result
# record when the kit result is successfully processed by this ETL
# routine. The routine finds new-to-it records to process by looking for
# kit results lacking this revision number in their log.  If a
# change to the ETL routine necessitates re-processing all kit results,
# this revision number should be incremented.
REVISION = 1


@etl.command("kit-result", help = __doc__)

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

def etl_kit_result(*, action: str):
    LOG.debug(f"Starting the kit result ETL routine, revision {REVISION}")

    db = DatabaseSession()

    #Fetch and iterate over kit results that aren't processed
    #
    # Rows we fetch are locked for update so that two instances of this
    # command don't try to process the same kit results.
    LOG.debug("Fetching unprocessed kit results")

    kit_results = db.cursor("kit_result")
    kit_results.execute("""
        select kit_result_id as id, document
          from receiving.kit_result
         where not processing_log @> %s
         order by id
            for update
        """, (Json([{ "revision" : REVISION }]),))

    processed_without_error = None

    try:
        for kit_result in kit_results:
            with db.savepoint(f"kit result {kit_result.id}"):
                LOG.info(f"Processing kit result {kit_result.id}")

                # Convert kit barcode to full identifier, ensuring it is
                # known and from the correct identifier set.
                kit_barcode = kit_result.document.pop("kit_barcode")
                kit_identifier = find_identifier(db, kit_barcode)

                if not kit_identifier:
                    LOG.warning(f"Skipping kit result with unknown kit barcode «{kit_barcode}»")
                    mark_skipped(db, kit_result.id)
                    continue

                kit = find_kit(db, kit_identifier.uuid)

                # Error out the kit-result etl process if no kit found
                # The kit-result etl process can try again starting with this
                # record next time with the idea that the kit will be created
                # by then.
                if not kit:
                    raise KitNotFoundError(f"No kit with «{kit_identifier.uuid}» found")

                result_type = kit_result.document.pop("type")

                if result_type == "strip":
                    LOG.debug("Processing results for test strip")

                    # Get test strip details from kit found in warehouse
                    kit_test_strip = kit.details.get("test_strip")

                    if not kit_test_strip:
                        LOG.warning(f"Kit {kit.id} does not contain any test-strip data.")
                        insert_kit_test_strip(db,
                            kit                 = kit,
                            test_strip_results  = kit_result.document,
                            identifier_set      = "test-strips-fluathome.org")

                    else:
                        test_strip_barcode = kit_result.document.pop("barcode")

                        if (test_strip_barcode and
                            test_strip_barcode.lower() != kit_test_strip.get("barcode")):
                            LOG.warning(f"Test strip barcode {test_strip_barcode} does not match " + \
                                        f"barcode found in kit details: {kit_test_strip.get('barcode')}")

                        # update kit details with test-strip results
                        update_kit_test_strip(db, kit, kit_result.document)

                else:
                    LOG.debug("Processing results for kit sample")

                    test_date = kit_result.document.pop("test_date", None)
                    sample_barcode = kit_result.document.pop("barcode")
                    sample_identifier = find_identifier(db, sample_barcode)

                    if not sample_identifier:
                        LOG.warning(f"Unknown {result_type} sample barcode {sample_barcode}")
                        mark_skipped(db, kit_result.id)
                        continue

                    sample = find_sample(db, sample_identifier.uuid)

                    assert sample.id in {kit.rdt_sample_id, kit.utm_sample_id}, \
                        (f"Found sample {sample.id} does not match samples " +
                         f"linked to kit: {kit.rdt_sample_id, kit.utm_sample_id}")

                    # Loop through test results
                    for test_result in kit_result.document:
                        target_result = get_target_result(kit_result.document[test_result])

                        if target_result is None:
                            LOG.warning(f"Unknown result response {kit_result.document[test_result]}")
                            continue

                        target = find_or_create_target(db,
                            identifier  = test_result,
                            control     = False)

                        test_identifier = generate_identifier(
                            test_target         = test_result,
                            sample_identifier   = sample.identifier)

                        upsert_presence_absence(db,
                            identifier  = test_identifier,
                            sample_id   = sample.id,
                            target_id   = target.id,
                            present     = bool(target_result),
                            details     = {"test_date": test_date})

                mark_processed(db, kit_result.id, {"status": "processed"})

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
                "Commit successfully processed kit results up to this point?"

            commit = click.confirm(ask_to_commit)
        else:
            commit = action == "commit"

        if commit:
            LOG.info(
                "Committing all changes" if processed_without_error else \
                "Committing successfully processed kit results up to this point")
            db.commit()

        else:
            LOG.info("Rolling back all changes; the database will not be modified")
            db.rollback()


def find_kit(db: DatabaseSession, identifier: str):
    """
    Find kit using *kit_identifier* and samples linked to the kit.
    """
    LOG.debug(f"Looking up kit «{identifier}»")

    kit = db.fetch_row("""
        select kit_id as id, identifier, rdt_sample_id, utm_sample_id, details
          from warehouse.kit
         where kit.identifier = %s
        """, (identifier,))

    return kit


def insert_kit_test_strip(db: DatabaseSession,
                          kit: dict,
                          test_strip_results: dict,
                          identifier_set: set):
    """
    Insert *test_strip* into existing *kit* details.
    """
    LOG.info(f"Inserting test strip details into kit {kit.id} details")

    test_strip_barcode = test_strip_results.pop("barcode")

    test_strip_details = {
        "barcode": test_strip_barcode,
        "uuid": None,
        "results": test_strip_results
    }

    test_strip_identifier = find_identifier(db, test_strip_barcode)

    if not test_strip_identifier:
        LOG.warning(f"Unknown test strip barcode «{test_strip_barcode}»")

    elif test_strip_identifier.set_name not in identifier_set:
        LOG.warning(f"Test strip barcode found in unexpected identifier set " + \
                    f"«{test_strip_identifier.set_name}», not {identifier_set}")

    else:
        test_strip_details.update({
            "barcode": test_strip_identifier.barcode ,
            "uuid": test_strip_identifier.uuid
        })

    kit = db.fetch_row("""
        update warehouse.kit
          set details = jsonb_insert(details, '{test_strip}', %s)
         where kit_id = %s
        returning kit_id as id
        """, (Json(test_strip_details), kit.id))

    assert kit.id, "Inserting test strip details affected no rows!"

    return kit


def update_kit_test_strip(db: DatabaseSession,
                          kit: dict,
                          test_strip: dict):
    """
    Update test strip details of existing kit.

    The provided *test_strip* is merged into the existing
    kit test strip details.
    """
    LOG.info(f"Updating kit {kit.id} «{kit.identifier}» test strip details")

    kit = db.fetch_row("""
        update warehouse.kit
          set details = jsonb_set(details, '{test_strip, results}', %s)
         where kit_id = %s
        returning kit_id as id
        """, (Json(test_strip), kit.id))

    assert kit.id, "Updating details affected no rows!"

    return kit


def find_sample(db: DatabaseSession, collection_identifier: str):
    """
    Find sample in warehouse using *collection_identifier*.
    """
    LOG.debug(f"Looking up sample with collection identifier «{collection_identifier}»")

    sample = db.fetch_row("""
        select sample_id as id, identifier
          from warehouse.sample
         where identifier = %s
        """, (collection_identifier,))

    if not sample:
        raise SampleNotFoundError(f"No sample found with collection identifier «{collection_identifier}»")

    LOG.info(f"Found sample {sample.id} «{sample.identifier}»")
    return sample


def get_target_result(result: str) -> bool:
    """
    Convert test result to a boolean
    """
    result_map = {
        "0": False,
        "1": True
    }

    test_result = result_map.get(result, None)

    return test_result


def generate_identifier(sample_identifier: str, test_target: str) -> str:
    """
    Generate an identifier for Cephied presence/absence tests.
    """
    return f"sample/{sample_identifier}/cephied_target/{test_target}"


def mark_skipped(db: DatabaseSession, kit_result_id: int) -> None:
    LOG.debug(f"Marking kit result record {kit_result_id} as skipped")
    mark_processed(db, kit_result_id, { "status": "skipped" })


def mark_processed(db, kit_result_id: int, entry = {}) -> None:
    LOG.debug(f"Appending to processing log of kit result record {kit_result_id}")

    data = {
        "kit_result_id": kit_result_id,
        "log_entry": Json({
            **entry,
            "revision": REVISION,
            "timestamp": datetime.now(timezone.utc),
        }),
    }

    with db.cursor() as cursor:
        cursor.execute("""
            update receiving.kit_result
                set processing_log = processing_log || %(log_entry)s
             where kit_result_id = %(kit_result_id)s
            """, data)

class KitNotFoundError(ValueError):
    """
    Raised when :function: `etl_kit_result` is unable to find an existing
    kit with the given identifier
    """
    pass


class SampleNotFoundError(ValueError):
    """
    Raised when :function: `find_sample` is unable to find an existing sample
    with the given identifier
    """
    pass
