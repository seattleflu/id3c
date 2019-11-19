"""
Upload data into the receiving area.

The receiving area of ID3C is mostly comprised of tables representing an
ordered log of JSON documents to process into the warehouse.

Typically there is a more tailored ID3C command to manipulate specific tables
in the receiving area (such as `id3c manifest` or `id3c redcap-det`), but
sometimes it is useful to more directly manipulate tables.  This is
particularly true for receiving tables which are generally populated through
the web API and do not have a more tailored command.
"""
import click
import logging
from id3c.cli import cli
from id3c.db.session import DatabaseSession


LOG = logging.getLogger(__name__)


@cli.group("receiving", help = __doc__)
def receiving():
    pass


@receiving.command("upload")

@click.argument("table_name", metavar = "<table>")

@click.argument("document_file",
    metavar = "<documents.ndjson>",
    type = click.File("r"))

def upload(table_name, document_file):
    """
    Upload documents into a receiving table.

    <table> must be the name of a table in the receiving schema which has a
    "document" column.  All other columns in the table must be optional on
    insert, as only "document" is provided.

    <documents.ndjson> must be a newline-delimited JSON file containing one
    document per line to insert as a table row.
    """
    db = DatabaseSession()

    try:
        LOG.info(f"Copying documents from {document_file.name}")

        row_count = db.copy_from_ndjson(("receiving", table_name, "document"), document_file)

        LOG.info(f"Received {row_count:,} {table_name} records")
        LOG.info("Committing all changes")
        db.commit()

    except:
        LOG.info("Rolling back all changes; the database will not be modified")
        db.rollback()
        raise
