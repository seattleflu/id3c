"""
Manage identifiers and barcodes.
"""
import click
import logging
import seattleflu.db as db
from seattleflu import labelmaker
from seattleflu.db.session import DatabaseSession
from seattleflu.db.cli.__main__ import cli


LOG = logging.getLogger(__name__)


@cli.group("identifier", help = __doc__)
def identifier():
    pass


# Mint subcommand
@identifier.command("mint")
@click.argument("set_name", metavar = "<set name>")
@click.argument("count", metavar = "<count>", type = int)

@click.option("--labels",
    help = "Generate barcode labels for the new identifiers and save them to the given file",
    metavar = "<file.pdf>",
    type = click.File("wb"))

@click.option("--quiet", "-q",
    help = "Suppress printing of new identifiers to stdout",
    is_flag = True,
    flag_value = True)

def mint(set_name, count, *, labels, quiet):
    """
    Mint new identifiers and make barcode labels.

    <set name> is an existing identifier set, e.g. as output by the `id3c
    identifier set ls` command.

    <count> is the number of new identifiers to mint.
    """
    session = DatabaseSession()
    minted = db.mint_identifiers(session, set_name, count)

    if not quiet:
        for identifier in minted:
            print(identifier.barcode, identifier.uuid, sep = "\t")

    if labels:
        layout = labelmaker.layout_identifiers(set_name, minted)
        pdf = labelmaker.generate_pdf(layout)
        labels.write(pdf)


# Set subcommands
@identifier.group("set")
def set_():
    """Manage identifier sets."""
    pass

@set_.command("ls")
def set_ls():
    """List identifier sets."""
    session = DatabaseSession()

    with session.cursor() as cursor:
        cursor.execute("""
            select name, description
              from warehouse.identifier_set
             order by lower(name)
            """)

        sets = list(cursor)

    # Line up names nicely into a column
    template = "{:<%d}" % (max(len(s.name) for s in sets) + 3)

    for set in sets:
        click.secho(template.format(set.name), bold = True, nl = False)
        click.echo(set.description)
