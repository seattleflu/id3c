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
from id3c.cli.redcap import Project, completion_status_field, is_complete, det
from id3c.db.session import DatabaseSession
from id3c.db.datatypes import as_json


LOG = logging.getLogger(__name__)


@cli.group("redcap-det", help = __doc__)
def redcap_det():
    pass


@redcap_det.command("generate")
@click.argument("record-ids", nargs = -1)

@click.option("--api-url",
    metavar = "<url>",
    help = "The API endpoint of the REDCap instance.",
    required = True,
    envvar = "REDCAP_API_URL",
    show_envvar = True)

@click.option("--project-id",
    metavar = "<id>",
    type = int,
    help = "The project id from which to fetch records.  "
           "Must match the project associated with the provided API token.",
    required = True,
    envvar = "REDCAP_PROJECT_ID",
    show_envvar = True)

@click.option("--token",
    metavar = "<token-name>",
    help = "The name of the environment variable that holds the API token.  "
           "Defaults to a name based on the --api-url and --project-id values: REDCAP_API_TOKEN_{api_url_origin}_{project_id}.")

@click.option("--since-date",
    metavar = "<since-date>",
    help = "Limit to REDCap records that have been created/modified since the given date. " +
           "Format must be YYYY-MM-DD HH:MM:SS (e.g. '2019-01-01 00:00:00')")

@click.option("--until-date",
    metavar = "<until-date>",
    help = "Limit to REDCap records that have been created/modified before the given date. " +
           "Format must be YYYY-MM-DD HH:MM:SS (e.g. '2019-01-01 00:00:00')")

@click.option("--instrument", "instruments",
    metavar = "<instrument-name>",
    help = "Limit generated DET notifications to the named instrument; may be used multiple times",
    multiple = True)

@click.option("--event", "events",
    metavar = "<event-name>",
    help = "Limit generated DET notifications to the named unique event (e.g. priority_arm_1); may be used multiple times",
    multiple = True)

@click.option("--include-incomplete",
    help = "Generate DET notifications for instruments marked as incomplete and unverified too, instead of only those marked complete",
    is_flag = True,
    flag_value = True)

def generate(record_ids: List[str], api_url: str, project_id: int, token: str, since_date: str, until_date: str,
    instruments: List[str], events: List[str], include_incomplete: bool):
    """
    Generate DET notifications for REDCap records.

    Specify one or more record ids to only consider those records.  If no
    record ids are given, then all records (or all records matching the date
    filters) are considered.  The REDCap API does not support combining a list
    of specific record ids with date filters, so this command does not either.

    DET notifications are output for all completed instruments for each record
    by default.  Pass --include-incomplete to output DET notifications for
    incomplete and unverified instruments too.  Pass one or more --instrument
    options to limit output to specific instrument names.  Pass one or more
    --event options to limit output to specific event names.

    All DET notifications are output to stdout as newline-delimited JSON
    records.  You will likely want to redirect stdout to a file.
    """
    api_token = os.environ[token] if token else None

    project = Project(api_url, project_id, token = api_token)

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

    if events:
        LOG.debug(f"Producing DET notifications for the following events: {events}")
        assert_known_attribute_value(project, 'events', events, 'event')
    else:
        LOG.debug(f"Producing DET notifications for all events ({project.events})")
        events = project.events

    if instruments:
        LOG.debug(f"Producing DET notifications for the following {'instruments' if include_incomplete else 'complete instruments'}: {instruments}")
        assert_known_attribute_value(project, 'instruments', instruments, 'instrument')
    else:
        LOG.debug(f"Producing DET notifications for all {'instruments' if include_incomplete else 'complete instruments'} ({project.instruments})")
        instruments = project.instruments

    fields = [
        project.record_id_field,
        *map(completion_status_field, instruments),
    ]

    records = project.records(
        since_date = since_date,
        until_date = until_date,
        ids = record_ids or None,
        fields = fields,
        events = events,
        raw = True)

    for record in records:
        for instrument in instruments:
            if include_incomplete or is_complete(instrument, record):
                print(as_json(det(project, record, instrument)))


def assert_known_attribute_value(project: Project, attribute: str, values: List[str], option: str=None):
    """
    Throws an :class:`Exception` if the given REDCap *project* contains no
    values for the given *attribute*.

    Throws an :class:`AssertionError` if any of the given *values* are not
    contained in the *attribute* of the given REDCap *project*.

    Provide an optional *option* value that is the name of the (unhyphenated)
    command option as presented to the user. If not provided, defaults to
    *attribute*.
    """
    known_values = getattr(project, attribute)
    if not known_values:
        raise Exception(f"There are no --{option} values in the REDCap project.")

    unknown_values = set(values) - set(known_values)

    if not option:
        option = attribute

    assert not unknown_values, \
        f"The following --{option} names aren't in the REDCap project: {unknown_values}"


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
