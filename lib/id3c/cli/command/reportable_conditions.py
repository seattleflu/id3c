"""
Run reportable condition notification routine.

Sends a nicely formatted Slack message for samples with reportable conditions as
described in the `warehouse.organism` table.

Searches for unseen reportable conditions in `shipping.reportable_condition_v1`,
and updates `warehouse.presence_absence` after sending a Slack alert.

Visit the Reportable Conditions Notifications Slack app
<https://api.slack.com/apps/ALJJAQGKH> to access the Slack Incoming Webhook
URLs <https://api.slack.com/apps/ALJJAQGKH/incoming-webhooks?>. These URLs must
be defined as environment variables outside of this command.

\b
Environment variables required:
    * SLACK_WEBHOOK_REPORTING_GENERAL: incoming webhook URL for sending Slack
        messages to the Seattle Flu Study #reporting-general channel
    * SLACK_WEBHOOK_REPORTING_CHILDRENS: incoming webhook URL for sending Slack
        messages to the Seattle Flu STudy #reporting-childrens channel
"""
import os
import json
import click
import logging
import requests
from typing import Mapping, List
from textwrap import dedent
from datetime import datetime, timezone
from psycopg2.extras import NamedTupleCursor
from id3c.cli import cli
from id3c.db.session import DatabaseSession
from id3c.db.datatypes import Json


LOG = logging.getLogger(__name__)
REVISION = 1


@cli.group("reportable-conditions", help = __doc__)
def reportable_conditions():
    pass


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
    LOG.debug(f"Starting the reportable conditions notification routine, revision {REVISION}")

    db = DatabaseSession()

    def webhook(suffix):
        return os.environ.get("SLACK_WEBHOOK_ALERTS_TEST") \
            or os.environ[f"SLACK_WEBHOOK_REPORTING_{suffix}"]

    SLACK_WEBHOOK_REPORTING_GENERAL = webhook("GENERAL")
    SLACK_WEBHOOK_REPORTING_CHILDRENS = webhook("CHILDRENS")

    childrens_sites = get_childrens_sites(db)

    # Fetch and iterate over reportable condition records that aren't processed
    #
    # Rows we fetch are locked for update so that two instances of this
    # command don't try to process the same reportable condition records.
    LOG.debug("Fetching unprocessed reportable conditions records")

    reportable_conditions = db.cursor("reportable_conditions")
    reportable_conditions.execute("""
        select reportable_condition_v1.*, presence_absence_id as id
            from shipping.reportable_condition_v1
            join warehouse.presence_absence using (presence_absence_id)
        where details @> %s is not true
        order by id
            for update of presence_absence;
        """, (Json({"reporting_log":[{ "revision": REVISION }]}),))

    processed_without_error = None

    try:
        for record in reportable_conditions:
            with db.savepoint(f"reportable condition presence_absence_id {record.id}"):
                LOG.info(f"Processing reportable condition, presence_absence_id «{record.id}»")

                if not record.site:
                    LOG.info(f"No site found for presence_absence_id «{record.id}». " +
                        "Inferring site from manifest data.")

                url = SLACK_WEBHOOK_REPORTING_GENERAL \
                    if record.site not in childrens_sites and record.sheet != 'SCH' \
                    else SLACK_WEBHOOK_REPORTING_CHILDRENS

                response = send_slack_post_request(record, url)

                if response.status_code == 200:
                    mark_processed(db, record.id, {"status": "sent Slack notification"})
                    LOG.info(f"Finished processing presence_absence_id «{record.id}»")

                else:
                    LOG.error(("Error: A Slack notification could not " \
                    f"be sent for presence_absence_id «{record.id}».\n" \
                    f"Slack API returned status code {response.status_code}: "\
                    f"{response.text}"))

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


def send_slack_post_request(record: NamedTupleCursor.Record, url: str) -> requests.Response:
    """
    Sends a POST request to a channel-specific Slack webhook *url*. The payload
    of this POST request is composed using Slack blocks. These blocks provide
    structure for a nicely formatted message that contains a link to
    Metabase plus relevant information from the given *record* from the
    database. The message contains, by request, a machine-friendly Json document
    containing minimal sample details.
    """
    data = {
        "sample": record.barcode,
        "site": record.site,
        "condition": record.lineage
    }

    if not record.site:
        data["manifest"] = {
            "workbook": record.workbook,
            "sheet": record.sheet,
        }

    payload = {
        "text": f":rotating_light: {record.lineage} detected",
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


def mark_processed(db, presence_absence_id: int, entry: Mapping) -> None:
    LOG.debug(dedent(f"""
    Marking reportable condition «{presence_absence_id}» as processed in the
    presence_absence table"""))

    data = {
        "presence_absence_id": presence_absence_id,
        "log_entry": Json({
            **entry,
            "revision": REVISION,
            "timestamp": datetime.now(timezone.utc),
        }),
    }

    with db.cursor() as cursor:
        cursor.execute("""
            update warehouse.presence_absence
               set details = jsonb_insert('{"reporting_log":[]}' || coalesce(details, '{}'), '{reporting_log, -1}', %(log_entry)s, insert_after => true)
             where presence_absence_id = %(presence_absence_id)s
            """, data)
