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
from datetime import date, datetime, timezone
from typing import Any, Optional
from id3c.cli.command import with_database_session
from id3c.db import find_identifier
from id3c.db.session import DatabaseSession
from id3c.db.datatypes import Json
from . import (
    etl,

    find_or_create_target,
    SampleNotFoundError,
    upsert_presence_absence,
)


LOG = logging.getLogger(__name__)


# This revision number is stored in the processing_log of each presence-absence
# record when the presence-absence test is successfully processed by this ETL
# routine. The routine finds new-to-it records to process by looking for
# presence-absence tests lacking this revision number in their log.  If a
# change to the ETL routine necessitates re-processing all presence-absence tests,
# this revision number should be incremented.
REVISION = 8


# Valid identifier.set_name values for the samples to be processed
valid_identifiers = [
        "samples",
        "collections-uw-tiny-swabs-home",
        "collections-uw-tiny-swabs-observed",
]

@etl.command("presence-absence", help = __doc__)
@with_database_session

def etl_presence_absence(*, db: DatabaseSession):
    LOG.debug(f"Starting the presence_absence ETL routine, revision {REVISION}")

    # Fetch and iterate over presence-absence tests that aren't processed
    #
    # Rows we fetch are locked for update so that two instances of this
    # command don't try to process the same presence-absence tests.
    LOG.debug("Fetching unprocessed presence-absence tests")

    presence_absence = db.cursor("presence_absence")
    presence_absence.itersize = 1
    presence_absence.execute("""
        select presence_absence_id as id, document,
               received::date as received_date
          from receiving.presence_absence
         where not processing_log @> %s
         order by id
           for update
        """, (Json([{ "revision": REVISION }]),))

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
                # Also skip old format to avoid ingesting wrong data from
                # plate swapped data! This will lead to 188 samples with the
                # wrong nwgc_id associated with them.
                #   -Jover, 06 Dec 2019
                if (group.document.get("store") is not None or
                    group.document.get("Update") is not None):

                    LOG.info("Skipping presence_absence record that is in old format")
                    mark_processed(db, group.id)
                    continue

                else:
                    raise error from None

            for received_sample in received_samples:
                received_sample_barcode = received_sample.get("investigatorId")
                if not received_sample_barcode:
                    LOG.info(f"Skipping sample «{received_sample['sampleId']}» without SFS barcode")
                    continue

                # Don't go any further if the sample is marked as Failed
                sample_failed = received_sample.get("sampleFailed")
                if sample_failed is True:
                    LOG.info(f"Skipping sample «{received_sample_barcode}» that has been failed")
                    continue

                # Don't go any further if there are no results to import.
                test_results = received_sample["targetResults"]

                if not test_results:
                    LOG.warning(f"Skipping sample «{received_sample_barcode}» without any results")
                    continue

                received_sample_id = str(received_sample["sampleId"])
                chip = received_sample.get("chip")
                extraction_date = received_sample.get("extractionDate")
                assay_name = received_sample.get("assayName")
                assay_date = received_sample.get("assayDate")
                # The assayType field will be removed after Samplify starts
                # sending us OpenArray results with target.clinicalStatus.
                #
                # kfay, 28 Dec 2020
                assay_type = received_sample.get("assayType")

                # Guard against empty chip values
                assert chip or "chip" not in received_sample, "Received bogus chip id"

                # Must be current results
                LOG.info(f"Processing sample «{received_sample_barcode}»")

                if not received_sample.get("isCurrentExpressionResult"):
                    LOG.warning(f"Skipping out-of-date results for sample «{received_sample_barcode}»")
                    continue

                # Barcode must match a known identifier
                received_sample_identifier = sample_identifier(db, received_sample_barcode)

                if not received_sample_identifier:
                    LOG.warning(f"Skipping results for sample without a known identifier «{received_sample_barcode}»")
                    continue

                # Track Samplify's internal ids for our samples, which is
                # unfortunately necessary for linking genomic data NWGC also
                # sends.
                sample = update_sample(db,
                    identifier = received_sample_identifier,
                    additional_details = sample_details(received_sample))

                # Finally, process all results.
                for test_result in test_results:
                    test_result_target_id = test_result["geneTarget"]
                    LOG.debug(f"Processing target «{test_result_target_id}» for \
                    sample «{received_sample_barcode}»")

                    # Skip this result if it's actually a non-result
                    present = target_present(test_result)

                    if present is ...:
                        LOG.debug(f"No test result for «{test_result_target_id}», skipping")
                        continue

                    # Most of the time we expect to see existing targets so a
                    # select-first approach makes the most sense to avoid useless
                    # updates.
                    target = find_or_create_target(db,
                        identifier = test_result_target_id,
                        control = target_control(test_result["controlStatus"]))

                    # The unique identifier for each result.  If chip is
                    # applicable, then it's included to differentiate the same
                    # sample being run on multiple chips (uncommon, but it
                    # happens).
                    if chip:
                        identifier = f"NWGC/{received_sample_id}/{target.identifier}/{chip}"
                    else:
                        identifier = f"NWGC/{received_sample_id}/{target.identifier}"

                    # Most of the time we expect to see new samples and new
                    # presence_absence tests, so an insert-first approach makes more sense.
                    # Presence-absence tests we see more than once are presumed to be
                    # corrections.
                    upsert_presence_absence(db,
                        identifier = identifier,
                        sample_id  = sample.id,
                        target_id  = target.id,
                        present    = present,
                        details    = presence_absence_details(test_result,
                                                              group.received_date,
                                                              chip,
                                                              extraction_date,
                                                              assay_name,
                                                              assay_date,
                                                              assay_type))

            mark_processed(db, group.id)

            LOG.info(f"Finished processing presence_absence group {group.id}")


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
        select sample_id as id, identifier, details
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

        update_details_nwgc_id(sample, additional_details)

        sample = db.fetch_row("""
            update warehouse.sample
               set details = coalesce(details, '{}') || %s
             where sample_id = %s
            returning sample_id as id, identifier
            """, (Json(additional_details), sample.id))

        assert sample.id, "Updating details affected no rows!"

    return sample


def update_details_nwgc_id(sample: Any, additional_details: dict) -> None:
    """
    Given a *sample* fetched from `warehouse.sample`,
    extend `sample.details.nwgc_id` to an array if needed.

    Add provided "nwgc_id" within *additional_details* to the existing array
    if it doesn't already exist
    """
    if not sample.details:
        return

    existing_nwgc_ids = sample.details.get("nwgc_id", [])
    new_nwgc_ids = additional_details["nwgc_id"]

    # Extend details.nwgc_id to an array
    if not isinstance(existing_nwgc_ids, list):
        existing_nwgc_ids = [existing_nwgc_ids]

    # Concatenate exisiting and new nwgc_ids and deduplicate
    additional_details["nwgc_id"] = list(set(existing_nwgc_ids + new_nwgc_ids))


def sample_identifier(db: DatabaseSession, barcode: str) -> Optional[str]:
    """
    Find corresponding UUID for scanned sample barcode within
    warehouse.identifier.
    """
    identifier = find_identifier(db, barcode)

    if identifier:
        assert identifier.set_name in valid_identifiers, \
            f"Identifier found in invalid set «{identifier.set_name}»"

    return identifier.uuid if identifier else None


def sample_details(document: dict) -> dict:
    """
    Capture NWGC sample ID.
    Capture details about the go/no-go sequencing call for this sample.
    """
    return {
        "nwgc_id": [document['sampleId']],
        "sequencing_call": {
            "comment": document['sampleComment'],
            "initial": document['initialProceedToSequencingCall'],
            "final": document["sampleProceedToSequencing"],
        },
    }

def presence_absence_details(document: dict,
                             received_date: date,
                             chip: Any = None,
                             extraction_date: Any = None,
                             assay_name: Any = None,
                             assay_date: Any = None,
                             assay_type: Any = None) -> dict:
    """
    Describe presence/absence details in a simple data structure designed to
    be used from SQL.

    Raises `AssertionError` if we find an unknown *assay_name* or unknown
    *assay_type*.
    """
    # Look for clinical status -- historically, this detail was stored at the
    # top level, but with the new clinical validation of OpenArray results, we
    # may have a mix of clinial and research results in one upload. Fall back to
    # the assayType if clinicalStatus is not available for this target.
    assay_type = document.get('clinicalStatus') or assay_type

    device = None

    if assay_name:
        assert assay_name in {'OpenArray', 'TaqmanQPCR'}, f"Found unknown assay name «{assay_name}»"
        device = assay_name
    elif chip:
        device = "OpenArray"

    if assay_type:
        assert assay_type in {'Clia', 'Research'}, f"Found unknown assay type «{assay_type}»"
    else:
        # 12 February 2021 was the date we first received `assayType` for OpenArray results.
        if received_date < datetime(2021, 2, 12).date():
            # Assumes anything with 4 wellResults is "Clia" and everything else
            # "Research" assays
            assay_type = 'Clia' if len(document['wellResults']) == 4 else 'Research'

        # The "4 well assumption" used for CLIA results may not apply to
        # OpenArray results. If no assay type is given, default to 'Research' to
        # avoid accidentally reporting a non-clinical result to participants.
        else:
            LOG.warning("No assay type found for target. Defaulting to «Research».")
            assay_type = 'Research'


    return {
        "device": device,
        "assay_date": assay_date,
        "assay_type": assay_type,
        "extraction_date": extraction_date,
        "replicates": document['wellResults']
    }

def target_present(test_result: dict) -> Any:
    """
    Returns a value for ``warehouse.presence_absence.present``
    for the given received *test_result*, or ``...``
    (``Ellipsis``) if the test should be skipped.

    Raises a :py:class:`ValueError` if a value cannot be determined.
    """
    status = (
           test_result.get("targetStatus")
        or test_result.get("sampleState")
    )

    mapping = {
        "Detected": True,
        "NotDetected": False,

        "Positive": True,
        "PositiveControlPass": True,
        "Negative": False,
        "Indeterminate": None,
        "Inconclusive": None,

        # These are valid _workflow_ statuses, but they're not really test
        # results; they describe the circumstances around performing the test,
        # not the result of the test itself.  We skip ingesting them for now as
        # there is no place for them in our current data model.
        #
        # I did consider making these map to None/null like Indeterminate.
        # That would make "present is null" results in the database mean "this
        # test was run, but the result is unknown due to circumstances left
        # unspecified".  I ultimately decided against it as the goal with ID3C
        # is to aim for simpler data models which are easier to reckon about,
        # not track everything that's performed like a LIMS/LIS does.
        #   -trs, 20 Mar 2020
        "Fail": ...,
        "Repeat": ...,
        "Review": ...,
    }

    if not status or status not in mapping:
        raise ValueError(f"Unable to determine target presence given «{test_result}»")

    return mapping[status]


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
