"""
Manage identifiers and barcodes.

ID3C can be a central identifier authority for studies through creation of
pre-generated, error-resistant, human- and computer-friendly barcodes for
tracking physical items, such as samples or kits.

Identifiers are version 4 UUIDs which are shortened to 8-character barcodes
while preserving a minimum substitution distance between all barcodes.  This
method of barcode generation is described in the literature as CualID.¹

Identifiers belong to named sets which serve to group identifiers by the kind
of thing the identifier will label.  Sets also determine how batches of new
barcodes are printed on physical labels (the label SKU and layout).

\b
¹ cual-id: Globally Unique, Correctable, and Human-Friendly Sample Identifiers
  for Comparative Omics Studies. John H. Chase, Evan Bolyen, Jai Ram Rideout,
  J. Gregory Caporaso mSystems Dec 2015, 1 (1) e00010-15;
  DOI: 10.1128/mSystems.00010-15
  <https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5069752/>
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

    If --labels are requested, a PDF of printable barcode labels is generated
    using the Lab Labels¹ instance <https://backoffice.seattleflu.org/labels/>.
    An alternative instance may be used by setting the LABEL_API environment
    variable to the instance URL.

    ¹ https://github.com/MullinsLab/Lab-Labels
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
