"""
Sync REDCap

Synchronize ID3C records with remote REDCap records via the logging API. Note, this
synchronization is one-way and acts similarly to a git pull: REDCap -> ID3C.

id3c redcap-sync <command>
Commands:
    - delete: Fetches deleted REDCap records and deletes them from ID3C to synchronize the
              database with any remote changes. May be run in `alert` mode to notify ID3C
              maintainers via Slack of any deletion events that should be looked into. May
              also be run in `quiet` mode (default) to only log deletion events to stdout.
"""
import json
import logging
import os

import click
import requests

from id3c.cli import cli
from id3c.cli.redcap import Project
from id3c.cli.command import with_database_session, with_redcap_project
from id3c.db import (
    delete_encounters,
    delete_encounter_locations_by_encounter,
    delete_individuals,
    delete_locations,
    delete_samples,
    delete_presence_absences_by_sample,
)
from id3c.db.session import DatabaseSession
from id3c.db.datatypes import Json

LOG = logging.getLogger(__name__)


@cli.group("redcap-sync", help=__doc__)
def redcap_sync():
    """Command group for REDCap Synchronization routines"""
    pass


@redcap_sync.command("delete")
@click.option(
    "--since-date",
    metavar="<since-date>",
    help="Limit to REDCap log events that have been generated since the given date. "
    + "Format must be YYYY-MM-DD HH:MM:SS (e.g. '2019-01-01 00:00:00')",
)
@click.option(
    "--until-date",
    metavar="<until-date>",
    help="Limit to REDCap log events that have been generated before the given date. "
    + "Format must be YYYY-MM-DD HH:MM:SS (e.g. '2019-01-01 00:00:00')",
)
@click.option(
    "--record-id",
    metavar="<record-id>",
    help="The REDCap record ID for which to fetch deletion events for",
)
@click.option(
    "--user",
    metavar="<user>",
    help="The REDCap user who generated these log events",
)
@click.option(
    "--dag",
    metavar="<dag>",
    help="The REDCap DAG that these log events belong to",
)
@click.option(
    "--return-format",
    metavar="<return-format>",
    help="The format in which to return REDCap error messages",
)
@click.option(
    "--post-to-slack",
    help="Post deletion identifiers to Slack",
    is_flag=True,
    flag_value=True,
)
@click.option(
    "--log-identifiers",
    help="Logs identifiers at the INFO level",
    is_flag=True,
    flag_value=True,
)
@with_redcap_project
@with_database_session
def delete(
    db: DatabaseSession,
    project: Project,
    since_date: str,
    until_date: str,
    record_id: str,
    user: str,
    dag: str,
    return_format: str,
    post_to_slack: bool,
    log_identifiers: bool,
):
    """
    Synchronizes ID3C records with REDCap remote records, or alerts ID3C maintainers to records
    that might need manual intervention.
    """
    LOG.info(
        f"Starting the REDCap record deletion routine for REDCap project #{project.id}: {project.title}."
    )

    # try to get slack env vars up front before doing any work
    if post_to_slack:
        slack_url = (
            os.environ.get("SLACK_WEBHOOK_ALERTS_TEST")
            or os.environ["SLACK_WEBHOOK_ID3C_ALERTS"]
        )

    if since_date and until_date:
        LOG.debug(
            f"Getting all records that have been deleted between {since_date} and {until_date}"
        )
    elif since_date:
        LOG.debug(f"Getting all records that have been deleted since {since_date}")
    elif until_date:
        LOG.debug(f"Getting all records that have been deleted before {until_date}")
    else:
        LOG.debug("No date range specified. Getting all deleted records")

    if record_id:
        LOG.debug(f"Limiting log events to those for record {record_id}")
    else:
        LOG.debug("Not limiting log events by record id")

    if user:
        LOG.debug(f"Limiting log events to those generated by user {user}")
    else:
        LOG.debug("Not limiting log events by user")

    if dag:
        LOG.debug(f"Limiting log events to those generated within DAG ID: {dag}")
    else:
        LOG.debug("Not limiting log events by DAG ID")

    deletion_events = project.logs(
        log_type="record_delete",
        since_date=since_date,
        until_date=until_date,
        record=record_id,
        user=user,
        dag=dag,
        return_format=return_format,
    )
    LOG.info(
        f"Fetched {len(deletion_events)} logged record deletion events via REDCap logging API"
    )

    deleted_redcap_record_identifiers = [
        f'{project.base_url}{project.id}/{record["record"]}/%'
        for record in deletion_events
    ]

    del_encounters = db.cursor("encounters")
    del_encounters.itersize = 1
    del_encounters.execute(
        """
            SELECT
                encounter_id, individual_id, identifier
            FROM
                warehouse.encounter
            WHERE
                encounter.identifier LIKE ANY (%s)
        """,
        (deleted_redcap_record_identifiers,),
    )

    if log_identifiers:
        LOG.info(deleted_redcap_record_identifiers)

    # Post any record deletion events to Slack to provide insight into
    # potential data deletions.
    if post_to_slack and deleted_redcap_record_identifiers:
        payload = {
            "text": "REDCap Logging API found records that may require deletion from ID3C",
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"There are *{len(deleted_redcap_record_identifiers)}* REDCap "
                        + "deletion events that might require deletion from ID3C. Each "
                        + "deletion event might be associated with more than one encounter. "
                        + "For brevity, encounter identifiers were excluded from this "
                        + "notification.\n\n Deletion events run by this routine will be "
                        + "rolled back. You should run the command below with rollback "
                        + "*enabled* (`--dry-run`) to verify that encounters it identifies "
                        + "as needing deletion and *all* their associated `individual`, "
                        + "`encounter_location`, `location`, `sample`, and `presence_absence` "
                        + "data should in fact be deleted (Note: `location` and `individual` "
                        + "records will not be deleted by this routine so long as they are linked "
                        + "to other encounters as well).\n\n To *permanently* delete the "
                        + "identifiers above and all associated data from ID3C, run the following "
                        + "command from within `id3c-production`:\n\n"
                        + "```PGSERVICE=seattleflu-production envdir ~/workspace/seattleflu/env.d/redcap"
                        + " pipenv run id3c redcap-sync delete "
                        + f"{f'--api-url {project.api_url} --project-id {project.id} '}"
                        + f"{f'--since-date {since_date} ' if since_date else ''}"
                        + f"{f'--until-date {until_date} ' if until_date else ''}"
                        + f"{f'--record-id {record_id} ' if record_id else ''}"
                        + f"{f'--user {user} ' if user else ''}"
                        + f"{f'--dag {dag} ' if dag else ''}"
                        + f"{f'--return-format {return_format} ' if return_format else ''}"
                        + "--commit```\n\n"
                        + "> *Please Note: This routine will not delete records from the ID3C receiving tables "
                        + "and bumping an ETL revision number may cause some of this data to be "
                        + "reingested.* If you would also like to remove records from receiving tables, "
                        + "you should do this manually. As an example, if you would like to remove records "
                        + "from the FHIR table, you might find records you need to delete with the command below \n\n"
                        + "```select * from receiving.fhir where document -> 'meta' ->> 'source' = "
                        + "'<encounter_identifier>'```\n\n"
                        + "You can find identifiers for records marked for deletion by using the "
                        + "`--log-identifiers` flag defined on the `redcap-sync` routine.",
                    },
                },
            ],
        }
        requests.post(
            slack_url,
            data=json.dumps(payload),
            headers={"Content-type": "application/json"},
            timeout=60,
        )

        LOG.debug("Record deletion message posted to Slack.")

    deletion_count = 0
    for encounter in del_encounters:
        LOG.info(
            f"Processing deleted encounter {encounter.encounter_id} from project {project.id}"
        )

        with db.savepoint(f"encounter {encounter.encounter_id}"):
            delete_linked_encounter_records(db, encounter.encounter_id)
            deletion_count += 1

        LOG.info(
            f"Finished processing deleted encounter {encounter.encounter_id} from project {project.id}"
        )

    LOG.info(
        f"Synced {len(deletion_events)} REDCap deletion events with ID3C. {deletion_count} ID3C encounters and associated data were removed."
    )


def delete_linked_encounter_records(db: DatabaseSession, encounter_id: int):
    """
    Deletes data associated with the linked `encounter_id` from the provided `db`.
    If other encounter_ids are linked to the same record as the provided `encounter_id`,
    don't delete those records.

    This function may alter data from the following tables and should be used with care:
    - `warehouse.encounter`
    - `warehouse.encounter_location`
    - `warehouse.individual`
    - `warehouse.location`
    - `warehouse.sample`
    - `warehouse.presence_absence`
    """
    LOG.info(f"Deleting all relational encounter data for encounter {encounter_id}")

    # Prefilter relational data to those identifiers in the location, individual, and sample
    # tables that are associated with ONLY our encounter. Samples are associated directly with
    # encounters, so we can select them directly. Individuals and locations may be linked to
    # multiple encounters.
    location_ids_unique_to_this_encounter = db.fetch_all(
        f"""
          SELECT
            location_id
          FROM
            warehouse.encounter_location t1
          WHERE
            encounter_id = {encounter_id}
            AND location_id NOT IN
              (
                SELECT
                  location_id
                FROM
                  warehouse.encounter_location
                WHERE
                  location_id = t1.location_id
                  AND encounter_id <> {encounter_id}
              )
        """
    )
    individual_ids_unique_to_this_encounter = db.fetch_all(
        f"""
          SELECT
            individual_id
          FROM
            warehouse.encounter t1
          WHERE
            encounter_id = {encounter_id}
            AND individual_id NOT IN
              (
                SELECT
                  individual_id
                FROM
                  warehouse.encounter
                WHERE
                  individual_id = t1.individual_id
                  AND encounter_id <> {encounter_id}
              )
        """
    )

    # delete encounter locations and wipe samples first to avoid FK errors
    # when deleting our encounter
    deleted_encounter_loc_rows = delete_encounter_locations_by_encounter(
        db, [encounter_id]
    )
    LOG.debug(
        f"Deleted {deleted_encounter_loc_rows} rows from `encounter_location` associated "
        + f"with `encounter_id` = {encounter_id}"
    )

    # We treat samples slightly differently since their provenance is the LIMS or an AQ sheet
    # rather than REDCap. Wipe any data we get from REDCap, namely: `encounter_id`` and
    # `details.note`. If we have only the keys `coding` and `note` in our details column,
    # that means the sample provenance was REDCap and this sample is safe to delete.
    sample_ids_unique_to_this_encounter = db.fetch_all(
        f"SELECT * FROM warehouse.sample WHERE encounter_id = {encounter_id}"
    )
    for sample in sample_ids_unique_to_this_encounter:
        if set(["coding", "note"]) == set(sample.details.keys()):
            deleted_sample_rows = delete_samples(db, [sample.sample_id])
            deleted_pa_rows = delete_presence_absences_by_sample(db, [sample.sample_id])
            LOG.debug(
                f"Deleted {deleted_sample_rows} samples and {deleted_pa_rows} "
                + "presence_absence records (sample provenance was REDCap)"
            )
        else:
            sample.details.pop("note")
            db.fetch_row(
                """
                UPDATE
                  warehouse.sample
                SET
                  encounter_id = NULL,
                  details = %(details)s
                WHERE
                  sample_id = %(sample_id)s
                  RETURNING sample_id
                """,
                {"sample_id": sample.sample_id, "details": Json(sample.details)},
            )
            LOG.debug(
                f"Disassociated encounter {encounter_id} from sample {sample.sample_id}"
            )

    deleted_encounter_rows = delete_encounters(db, [encounter_id])
    assert deleted_encounter_rows == 1, "The number of encounters deleted was not 1"

    # delete locations and individuals after deleting encounters to avoid FK errors
    # when deleting them
    locations_to_delete = [
        location.location_id for location in location_ids_unique_to_this_encounter
    ]
    deleted_location_rows = delete_locations(db, locations_to_delete)
    LOG.debug(
        f"Deleted {deleted_location_rows} rows from `location` associated with "
        + f"`encounter_id` = {encounter_id}"
    )

    individuals_to_delete = [
        individual.individual_id for individual in individual_ids_unique_to_this_encounter
    ]
    deleted_individual_rows = delete_individuals(db, individuals_to_delete)
    LOG.debug(
        f"Deleted {deleted_individual_rows} rows from `individual` associated with "
        + f"`encounter_id` = {encounter_id}"
    )

    LOG.debug(
        f"Successfully deleted encounter {encounter_id} and dissassociated all relational data"
    )
