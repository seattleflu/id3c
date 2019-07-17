"""
Create alerts for reportable conditions

Custom cronjob — Small bit of Python we can keep in
seattleflu/id3c-customizations and run on backoffice. It can keep a persistent
record of the reportable (sample identifier, organism lineage) tuples it has
seen so far in a custom table in the database. To send a nice Slack message,
this will have to make a web request to the Slack webhook endpoint.

Writes to a persistent record of reportable conditions it has seen so far in a
custom table in the database (shipping.reportable_condition).

Sends a nicely formatted Slack message to a Slack webhook endpoint
(<https://api.slack.com/apps>) for the appropriate reporting channels.

"""
import os
import json
import click
import logging
import requests
from typing import List
from textwrap import dedent
from datetime import datetime, timezone
from psycopg2.extras import NamedTupleCursor
from seattleflu.db.cli import cli
from seattleflu.db.session import DatabaseSession
from seattleflu.db.datatypes import Json


LOG = logging.getLogger(__name__)
REVISION = 1


@cli.group("reportable-conditions", help = __doc__)
def reportable_conditions():
    pass

@reportable_conditions.command("search")
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


def search(*, action: str):
    """
    Find and insert new samples with postive results for reportable conditions
    as defined by warehouse.organism.
    """

    LOG.debug("Starting the reportable conditions routine")

    db = DatabaseSession()

    reportable_conditions = db.fetch_all("""
        with reportable as (
            select array_agg(lineage) as lineages
            from warehouse.organism
            where details @> '{"report_to_public_health":true}'
        )

        select
            organism.lineage::text as lineage,
            sample.identifier as identifier,
            site.identifier as site

        from warehouse.presence_absence
        join warehouse.target using (target_id)
        join warehouse.organism using (organism_id)
        join warehouse.sample using (sample_id)
        left join warehouse.encounter using (encounter_id)
        left join warehouse.site using (site_id)

        where organism.lineage <@ (table reportable)
        and present

        order by encountered desc
        """, )

    processed_without_error = None

    try:
        for record in reportable_conditions:
            with db.savepoint(f"sample {record.identifier}"):
                LOG.info(f"Processing reportable condition «{record.identifier}»")
                find_or_create_reportable_sample(db, record)


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
                "Commit successfully processed reportable conditions up to this point?"

            commit = click.confirm(ask_to_commit)
        else:
            commit = action == "commit"

        if commit:
            LOG.info(
                "Committing all changes" if processed_without_error else \
                "Committing successfully processed reportable conditions up to this point")
            db.commit()

        else:
            LOG.info("Rolling back all changes; the database will not be modified")
            db.rollback()


def find_or_create_reportable_sample(db: DatabaseSession, record: dict):
    """
    Insert reportable condition plus related sample details if it doesn't exist.
    """
    identifier = record.identifier
    lineage = record.lineage
    site = record.site

    LOG.debug(f"Looking up sample «{identifier}», organism of type «{lineage}»")

    reportable_condition = db.fetch_row("""
        select identifier, lineage, site
          from shipping.reportable_condition
        where identifier = %s and lineage = %s
        """, (identifier, lineage))

    if reportable_condition:
        LOG.info(f"Found sample «{identifier}», organism of type «{lineage}»")
    else:
        data = {
            "identifier": identifier,
            "lineage": lineage,
            "site": site,
        }

        reportable_condition = db.fetch_row("""
            insert into shipping.reportable_condition (identifier, lineage, site)
                values (%(identifier)s, %(lineage)s, %(site)s)
            returning identifier, lineage, site
            """, data)

        LOG.info(f"Created sample «{identifier}», organism of type «{lineage}»")

    return reportable_condition


@reportable_conditions.command("notify")

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

def notify(*, action: str):
    """
    Contains some hard-coded
    """
    LOG.debug(f"Starting the reportable conditions notification routine, revision {REVISION}")

    db = DatabaseSession()

    SLACK_WEBHOOK_URL = os.environ['SLACK_WEBHOOK_URL']  # Testing only
    SLACK_WEBHOOK_REPORTING_GENERAL = os.environ['SLACK_WEBHOOK_REPORTING_GENERAL']
    SLACK_WEBHOOK_REPORTING_CHILDRENS = os.environ['SLACK_WEBHOOK_REPORTING_CHILDRENS']

    childrens_sites = get_childrens_sites(db)

    # Fetch and iterate over reportable condition records that aren't processed
    #
    # Rows we fetch are locked for update so that two instances of this
    # command don't try to process the same reportable condition records.
    LOG.debug("Fetching unprocessed reportable conditions records")

    reportable_conditions = db.cursor("reportable_conditions")
    reportable_conditions.execute("""
        select reportable_condition_id as id,
          identifier, lineage, site
          from shipping.reportable_condition
         where not processing_log @> %s
         order by id
           for update
        """, (Json([{ "revision": REVISION }]),))

    processed_without_error = None

    try:
        for record in reportable_conditions:
            with db.savepoint(f"reportable conditions record {record.id}"):
                LOG.info(f"Processing reportable condition record {record.id}")

                url = SLACK_WEBHOOK_REPORTING_GENERAL \
                    if record.site not in childrens_sites \
                    else SLACK_WEBHOOK_REPORTING_CHILDRENS

                # TODO replace SLACK_WEBHOOK_URL with url
                response = send_slack_post_request(record, SLACK_WEBHOOK_URL)

                if response.status_code == 200:
                    mark_processed(db, record.id, {"status": "processed"})
                    LOG.info(f"Finished processing reportable condition record {record.id}")

                else:
                    LOG.warning(("Error: A Slack notification could not " \
                    f"be sent for reportable condition id «{record.id}»"))

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
                "Commit successfully processed reportable condition records up to this point?"

            commit = click.confirm(ask_to_commit)
        else:
            commit = action == "commit"

        if commit:
            LOG.info(
                "Committing all changes" if processed_without_error else \
                "Committing successfully processed reportable condition records up to this point")
            db.commit()

        else:
            LOG.info("Rolling back all changes; the database will not be modified")
            db.rollback()


def get_childrens_sites(db) -> List:
    """Gets all sites from the warehouse whose name contains 'Childrens'"""

    childrens_sites = db.fetch_all("""
        select identifier
        from warehouse.site
        where identifier like '%Childrens%'
        """, )
    return [site.identifier for site in childrens_sites]


def send_slack_post_request(record: NamedTupleCursor.Record, url: str) -> requests.PreparedRequest:
    """
    Sends a POST request to a channel-specific Slack webhook *url*. The payload
    of this POST request is composed using Slack blocks. These blocks provide
    structure for a nicely formatted message that contains a link to
    Metabase plus relevant information from the given *record* from the
    database. The message contains, by request, a machine-friendly Json document
    containing minimal sample details.
    """
    data = {
        "sample": record.identifier[:8],
        "site": record.site,
        "condition": record.lineage
    }

    payload = {
        "text": dedent("""
        A reportable condition was detected, but there was a problem sending the
        message
        """),
        "blocks": [{
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": dedent(f"""
                :rotating_light: @channel {record.lineage} detected. \n
                *<https://backoffice.seattleflu.org/metabase/question/55|Go to Metabase>*
                """)
            }
        },
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Details:*\n```{json.dumps(data, sort_keys=True, indent=4)}```"
                }
            ]
        }]
    }

    return requests.post(url, data=json.dumps(payload),
                         headers={'Content-type': 'application/json'})


def mark_processed(db, reportable_condition_id: int, entry: {}) -> None:
    LOG.debug(f"Marking reportable condition «{reportable_condition_id}» as processed")

    data = {
        "reportable_condition_id": reportable_condition_id,
        "log_entry": Json({
            **entry,
            "revision": REVISION,
            "timestamp": datetime.now(timezone.utc),
        }),
    }

    with db.cursor() as cursor:
        cursor.execute("""
            update shipping.reportable_condition
               set processing_log = processing_log || %(log_entry)s
             where reportable_condition_id = %(reportable_condition_id)s
            """, data)
