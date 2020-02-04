"""
Generate and upload REDCap DET notifications.

This command is useful for backfilling data entry trigger (DET) notifications
for project records which missed the normal trigger process.  Two common
scenarios when this happens are:

1. The records were created/modified before trigger was enabled.

\b
2. The records were imported via REDCap's API or mobile app, which
   bypasses the trigger.
"""
import os
import click
import logging
from typing import List
from id3c.cli import cli
from id3c.cli.redcap import Project, is_complete
from id3c.db.session import DatabaseSession
from id3c.db.datatypes import as_json


LOG = logging.getLogger(__name__)


@cli.group("redcap-det", help = __doc__)
def redcap_det():
    pass


@redcap_det.command("generate")
@click.argument("record-ids", nargs = -1)

@click.option("--project-id",
    metavar = "<id>",
    type = int,
    help = "The project id from which to fetch records.  "
           "Used as a sanity check that the correct API token is provided.",
    required = True)

@click.option("--token-name",
    metavar = "<token-name>",
    help = "The name of the environment variable that holds the API token",
    default = "REDCAP_API_TOKEN")

@click.option("--since-date",
    metavar = "<since-date>",
    help = "Limit to REDCap records that have been created/modified since the given date. " +
           "Format must be YYYY-MM-DD HH:MM:SS (e.g. '2019-01-01 00:00:00')")

@click.option("--until-date",
    metavar = "<until-date>",
    help = "Limit to REDCap records that have been created/modified before the given date. " +
           "Format must be YYYY-MM-DD HH:MM:SS (e.g. '2019-01-01 00:00:00')")

@click.option("--include-incomplete",
    help = "Generate DET notifications for instruments marked as incomplete and unverified too, instead of only those marked complete",
    is_flag = True,
    flag_value = True)

def generate(record_ids: List[str], project_id: int, token_name: str, since_date: str, until_date: str, include_incomplete: bool):
    """
    Generate DET notifications for REDCap records.

    Specify one or more record ids to only consider those records.  If no
    record ids are given, then all records (or all records matching the date
    filters) are considered.  The REDCap API does not support combining a list
    of specific record ids with date filters, so this command does not either.

    Requires environmental variables REDCAP_API_URL and REDCAP_API_TOKEN (or
    whatever you passed to --token-name).

    DET notifications are output for all completed instruments for each record
    by default.  Pass --include-incomplete to output DET notifications for
    incomplete and unverified instruments too.

    All DET notifications are output to stdout as newline-delimited JSON
    records.  You will likely want to redirect stdout to a file.
    """
    api_token = os.environ[token_name]
    api_url = os.environ['REDCAP_API_URL']

    project = Project(api_url, api_token, project_id)

    LOG.info(f"REDCap project #{project.id}: {project.title}")

    if bool(since_date or until_date) and bool(record_ids):
        raise click.UsageError("The REDCap API does not support fetching records filtered by id *and* date.")

    if since_date and until_date:
        LOG.debug(f"Getting all records that have been created/modified between {since_date} and {until_date}")
    elif since_date:
        LOG.debug(f"Getting all records that have been created/modified since {since_date}")
    elif until_date:
        LOG.debug(f"Getting all records that have been created/modified before {until_date}")
    elif record_ids:
        LOG.debug(f"Getting specified records: {record_ids}")
    else:
        LOG.debug(f"Getting all records")

    records = project.records(
        since_date = since_date,
        until_date = until_date,
        ids = record_ids or None,
        raw = True)

    for record in records:
        for instrument in project.instruments:
            if include_incomplete or is_complete(instrument, record):
                print(as_json(create_det_records(project, record, instrument)))


def create_det_records(project: Project, record: dict, instrument: str) -> dict:
    """
    Create a "fake" DET notification that mimics the format of REDCap DETs:

    \b
    {
        'redcap_url',
        'project_id',
        'record',
        'instrument',
        '<instrument>_complete',
        'redcap_event_name',      // for longitudinal projects only
        'redcap_repeat_instance',
        'redcap_repeat_instrument',
    }
    """
    instrument_complete = instrument + '_complete'

    det_record = {
       'redcap_url': project.base_url,
       'project_id': str(project.id),                   # REDCap DETs send project_id as a string
       'record': str(record[project.record_id_field]),  # ...and record as well.
       'instrument': instrument,
       instrument_complete: record[instrument_complete],
       'redcap_repeat_instance': record.get('redcap_repeat_instance'),
       'redcap_repeat_instrument': record.get('redcap_repeat_instrument'),
       '__generated_by__': 'id3c',
    }

    if 'redcap_event_name' in record:
        det_record['redcap_event_name'] = record['redcap_event_name']

    return det_record


@redcap_det.command("upload")

@click.argument("det_file",
    metavar = "<det.ndjson>",
    type = click.File("r"))

def upload(det_file):
    """
    Upload REDCap DET notifications into database receiving area.

    <det.ndjson> must be a newline-delimited JSON file produced by this
    command's sibling command.
    """
    db = DatabaseSession()

    try:
        LOG.info(f"Copying REDCap DET records from {det_file.name}")

        row_count = db.copy_from_ndjson(("receiving", "redcap_det", "document"), det_file)

        LOG.info(f"Received {row_count:,} DET records")
        LOG.info("Committing all changes")
        db.commit()

    except:
        LOG.info("Rolling back all changes; the database will not be modified")
        db.rollback()
        raise
