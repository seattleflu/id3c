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
import requests
from typing import List
from seattleflu.db.cli import cli
from seattleflu.db.session import DatabaseSession
from seattleflu.db.datatypes import as_json

LOG = logging.getLogger(__name__)

@cli.group("redcap-det", help = __doc__)
def redcap_det():
    pass

@redcap_det.command("generate")
@click.option("--project-id",
    metavar = "<project-id>",
    help = "The project ID for the REDCap project",
    required = True)
@click.option("--token-name",
    metavar = "<token-name>",
    help = "The name of the environment variable that holds the API token",
    default = "REDCAP_API_TOKEN")
@click.option("--start-date",
    metavar = "<start-date>",
    help = "The start date of REDCap records that have been created/modified. " +
           "Format must be in YYYY-MM-DD HH:MM:SS (e.g.'2019-01-01 00:00:00')",
    required = True)

def generate(project_id: str, token_name: str, start_date: str):
    """
    Generate DET notifications for REDCap records.

    Requires environmental variables REDCAP_API_URL and REDCAP_API_TOKEN (or
    whatever you passed to --token-name).

    DET notifications are only output for completed instruments.

    All DET notifications are output to stdout as newline-delimited JSON
    records.  You will likely want to redirect stdout to a file.
    """
    LOG.debug(f"Getting all Kiosk Enrollment REDCap records that have been created/modified since {start_date}")

    api_token = os.environ[token_name]
    api_url = os.environ['REDCAP_API_URL']

    # Assuming that the base url for a REDCap instance is
    # just removing the 'api' from the API URL
    redcap_url = api_url.rstrip('/').replace('api', '')

    instruments = get_project_instruments(api_url, api_token)
    redcap_records = get_redcap_records(api_url, api_token, start_date)

    for record in redcap_records:
        # Find all instruments within a record that have been mark completed
        for instrument in instruments:
            if record[(instrument + '_complete')] == '2':
                print(as_json(create_det_records(redcap_url, project_id, record, instrument)))


def get_project_instruments(api_url: str, api_token: str) -> List[str]:
    """
    Get REDCap instruments for a given project, which is determined by the
    *api_token*
    """
    parameters = {
        'content': 'instrument',
        'format': 'json',
        'token': api_token
    }

    instruments = get_redcap_data(api_url, parameters)
    instrument_names = []

    for instrument in instruments:
        instrument_names.append(instrument['instrument_name'])

    return instrument_names


def get_redcap_records(api_url: str, api_token: str, start_date: str) -> List[dict]:
    """
    Get REDCap records for a given project, which is determined by the
    *api_token*
    """
    parameters = {
        'content': 'record',
        'format': 'json',
        'type': 'flat',
        'token': api_token,
        'dateRangeBegin': start_date
    }

    records = get_redcap_data(api_url, parameters)
    return records


def get_redcap_data(api_url: str, parameters: dict) -> List[dict]:
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

    response = requests.post(api_url, data=parameters, headers=headers)

    if response.status_code != 200:
        raise Exception(f"REDCap returned response status code {response.status_code}")

    return response.json()


def create_det_records(redcap_url: str, project_id: str,
                       record: dict, instrument: str) -> dict:
    """
    Create a "fake" DET notification that matches the format of REDCap DETs:

    \b
    {
        'redcap_url',
        'project_id',
        'record',
        'instrument',
        '<instrument>_complete',
        'redcap_event_name' (for longitudinal projects only)
    }
    """
    instrument_complete = instrument + '_complete'

    det_record = {
       'redcap_url': redcap_url,
       'project_id': project_id,
       'record': record['record_id'],
       'instrument': instrument,
       instrument_complete: record[instrument_complete]
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
