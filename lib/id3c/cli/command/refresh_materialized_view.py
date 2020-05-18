"""
Refresh materialized view in ID3C.
"""
import click
import logging
from id3c.cli import cli
from id3c.cli.command import with_database_session
from id3c.db.session import DatabaseSession


LOG = logging.getLogger(__name__)


@cli.command("refresh-materialized-view")
@with_database_session

@click.argument("schema-name",
    metavar = "<schema>",
    nargs = 1)
@click.argument("view-name",
    metavar = "<view>",
    nargs = 1)

def refresh_materialized_view(schema_name, view_name, db: DatabaseSession):
    """
    Refresh materialized view <schema>.<view> in ID3C.
    """

    LOG.info(f"Refreshing materialized view «{schema_name}.{view_name}»")

    db.cursor("refresh materialized view").execute("""
        select refresh_materialized_view(%s, %s)
    """, (schema_name, view_name ))

    LOG.info("Successfully refreshed materialized view")

