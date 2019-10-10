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
import re
import requests
from typing import Any, List
from seattleflu.db.cli import cli
from seattleflu.db.session import DatabaseSession
from seattleflu.db.datatypes import as_json

LOG = logging.getLogger(__name__)

@cli.group("redcap-det", help = __doc__)
def redcap_det():
    pass

@redcap_det.command("generate")
@click.option("--token-name",
    metavar = "<token-name>",
    help = "The name of the environment variable that holds the API token",
    default = "REDCAP_API_TOKEN")
@click.option("--since-date",
    metavar = "<since-date>",
    help = "Limit to REDCap records that have been created/modified since the given date. " +
           "Format must be YYYY-MM-DD HH:MM:SS (e.g. '2019-01-01 00:00:00')")

def generate(token_name: str, since_date: str):
    """
    Generate DET notifications for REDCap records.

    Requires environmental variables REDCAP_API_URL and REDCAP_API_TOKEN (or
    whatever you passed to --token-name).

    DET notifications are only output for completed instruments.

    All DET notifications are output to stdout as newline-delimited JSON
    records.  You will likely want to redirect stdout to a file.
    """
    api_token = os.environ[token_name]
    api_url = os.environ['REDCAP_API_URL']

    # Assuming that the base url for a REDCap instance is just removing the
    # trailing 'api' from the API URL
    redcap_url = re.sub(r'api/?$', '', api_url)

    project = get_project(api_url, api_token)

    LOG.info(f"REDCap project #{project['project_id']}: {project['project_title']}")

    if since_date:
        LOG.debug(f"Getting all records that have been created/modified since {since_date}")
    else:
        LOG.debug(f"Getting all records")

    instruments = get_project_instruments(api_url, api_token)
    redcap_records = get_redcap_records(api_url, api_token, since_date)

    for record in redcap_records:
        # Find all instruments within a record that have been mark completed
        for instrument in instruments:
            if record[(instrument + '_complete')] == '2':
                print(as_json(create_det_records(redcap_url, project, record, instrument)))


def get_project(api_url: str, api_token: str) -> dict:
    """
    Get REDCap project information, which is determined by the *api_token*.
    """
    return get_redcap_data(api_url, api_token, {"content": "project"})


def get_project_instruments(api_url: str, api_token: str) -> List[str]:
    """
    Get REDCap instruments for a given project, which is determined by the
    *api_token*
    """
    instruments = get_redcap_data(api_url, api_token, {"content": "instrument"})
    instrument_names = []

    for instrument in instruments:
        instrument_names.append(instrument['instrument_name'])

    return instrument_names


def get_redcap_records(api_url: str, api_token: str, since_date: str = None) -> List[dict]:
    """
    Get REDCap records for a given project, which is determined by the
    *api_token*
    """
    parameters = {
        'content': 'record',
        'type': 'flat',
    }

    if since_date:
        parameters['dateRangeBegin'] = since_date

    return get_redcap_data(api_url, api_token, parameters)


def get_redcap_data(api_url: str, api_token: str, parameters: dict) -> Any:
    """
    Get REDCap data by POST request to the REDCap API.

    *api_url* specifies instance of REDCap and `api_token` within *parameters*
    specifies the project.

    Consult REDCap API documentation for required and optional parameters
    to include in API request.
    """
    headers = {
        'Content-type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json'
    }

    data = {
        **parameters,
        'token': api_token,
        'format': 'json',
    }

    response = requests.post(api_url, data=data, headers=headers)
    response.raise_for_status()

    return response.json()


def create_det_records(redcap_url: str, project: dict,
                       record: dict, instrument: str) -> dict:
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
       'redcap_url': redcap_url,
       'project_id': project['project_id'],
       'record': record['record_id'],
       'instrument': instrument,
       instrument_complete: record[instrument_complete],
       'redcap_repeat_instance': record['redcap_repeat_instance'],
       'redcap_repeat_instrument': record['redcap_repeat_instrument'],
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
