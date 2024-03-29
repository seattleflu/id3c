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
import id3c.db as db
from id3c import labelmaker
from id3c.db.session import DatabaseSession
from id3c.cli import cli


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

@click.option("--layout",
    metavar = "<layout>",
    default = 'default',
    help = "Specify a desired label layout.")

@click.option("--copies-per-barcode",
    type = click.Choice(['1','2']),
    help = "Specify a desired number of copies per barcode. "
           "Currently only allows values 1 or 2 to avoid bad UX for the label PDFs.")

@click.option("--quiet", "-q",
    help = "Suppress printing of new identifiers to stdout",
    is_flag = True,
    flag_value = True)

@click.option("--dry-run",
    help = "Only go through the motions of changing the database; useful for benchmarking minting rates",
    is_flag = True,
    flag_value = True)


def mint(set_name, count, *, labels, layout, copies_per_barcode, quiet, dry_run):
    """
    Mint new identifiers and make barcode labels.

    <set name> is an existing identifier set, e.g. as output by the `id3c
    identifier set ls` command.

    <count> is the number of new identifiers to mint.

    If --labels are requested, a PDF of printable barcode labels is generated
    using the Lab Labels¹ instance <https://backoffice.seattleflu.org/labels/>.
    An alternative instance may be used by setting the LABEL_API environment
    variable to the instance URL.

    If --layout is requested, the printable barcode labels will use the given
    version of the layout, if available.

    ¹ https://github.com/MullinsLab/Lab-Labels
    """
    session = DatabaseSession()

    with session:
        minted = db.mint_identifiers(session, set_name, count)

        if dry_run:
            LOG.info("Rolling back all changes; the database will not be modified")
            session.rollback()

    if not quiet:
        for identifier in minted:
            print(identifier.barcode, identifier.uuid, sep = "\t")

    if labels:
        label_layout = labelmaker.layout_identifiers(set_name, minted, layout, copies_per_barcode)
        pdf = labelmaker.generate_pdf(label_layout)
        labels.write(pdf)


# Labels subcommand
@identifier.command("labels")
@click.argument("filename",
    metavar = "<file.pdf>",
    type = click.File("wb"))

@click.option("--layout",
    metavar = "<layout>",
    default = 'default',
    help = "Specify a desired label layout.")

@click.option("--copies-per-barcode",
    type = click.Choice(['1','2']),
    help = "Specify a desired number of copies per barcode. "
           "Currently only allows values 1 or 2 to avoid bad UX for the label PDFs.")


def labels(filename, layout: str='default', copies_per_barcode: int=None):
    """
    Make barcode labels for an existing batch of identifiers.

    A PDF of printable barcode labels is generated using the Lab Labels¹
    instance <https://backoffice.seattleflu.org/labels/>.  An alternative
    instance may be used by setting the LABEL_API environment variable to the
    instance URL.

    The batch of identifiers to make labels for is selected interactively based
    on the identifier set and time of original generation.

    If --layout is requested, the printable barcode labels will use the given
    version of the layout, if available.

    The option --copies-per-barcode can be provded to change the number of
    copies per barcode in the printable barcode labels.

    ¹ https://github.com/MullinsLab/Lab-Labels
    """
    session = DatabaseSession()

    # Fetch batches of identifiers
    with session.cursor() as cursor:
        cursor.execute("""
            select identifier_set.name as set_name,
                   to_char(generated, 'FMDD Mon YYYY') as generated_date,
                   generated,
                   count(*)
              from warehouse.identifier
              join warehouse.identifier_set using (identifier_set_id)
             group by generated, identifier_set.name
             order by generated, identifier_set.name
            """)

        batches = list(cursor)

    # Print batches for selection
    click.echo("\nThe following batches of identifiers exist:\n")

    for index, batch in enumerate(batches, 1):
        click.secho(f"{index:2d}) ", bold = True, nl = False)
        click.echo(f"{batch.generated_date:>11} — {batch.count:>5,} {batch.set_name}")

    # Which batch do we want?
    choice = click.prompt(
        "\nFor which batch would you like to make labels",
        prompt_suffix = "? ",
        type = click.IntRange(1, len(batches)),
        default = str(len(batches)))

    chosen_batch = batches[int(choice) - 1]

    # Fetch identifiers for the chosen batch
    with session.cursor() as cursor:
        cursor.execute("""
            select uuid, barcode
              from warehouse.identifier
              join warehouse.identifier_set using (identifier_set_id)
             where identifier_set.name = %s
               and generated = %s
            """, (chosen_batch.set_name, chosen_batch.generated))

        identifiers = list(cursor)

    assert len(identifiers) == chosen_batch.count

    label_layout = labelmaker.layout_identifiers(chosen_batch.set_name, identifiers, layout, copies_per_barcode)
    pdf = labelmaker.generate_pdf(label_layout)
    filename.write(pdf)


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
            select name, use, description
              from warehouse.identifier_set
             order by lower(name)
            """)

        sets = list(cursor)

    # Line up names and uses nicely into columns
    template_name = "{:<%d}" % (max(len(s.name) for s in sets) + 3)
    template_use = "{:<%d}" % (max(len(s.use) for s in sets) + 3)

    for set in sets:
        click.secho(template_name.format(set.name), bold = True, nl = False)
        click.secho(template_use.format(set.use), nl = False)
        click.echo(set.description)


@set_.command("create")
@click.argument("name", metavar = "<name>")
@click.argument("use", metavar="<use>")
@click.argument("description", metavar = "<description>")

def set_create(name, use, description):
    """
    Create a new identifier set.

    \b
    <name> is the name of the new set.
    <use> is the use classification for this set, valid values can be found using `id3c identifier set-use ls` command.
    <description> is a comment explaining the purpose of the set.
    """
    session = DatabaseSession()

    with session:
        identifier_set_id, = session.fetch_row("""
            insert into warehouse.identifier_set (name, use, description)
            values (%s, %s, %s)
            returning identifier_set_id
            """, (name, use, description))

    click.echo(
        "Created identifier set " +
        click.style(name, bold = True) +
        f" (#{identifier_set_id})")

# Set use subcommands
@identifier.group("set-use")
def set_use_():
    """Manage identifier set uses."""
    pass


@set_use_.command("ls")
def set_use_ls():
    """List identifier set uses."""
    session = DatabaseSession()

    with session.cursor() as cursor:
        cursor.execute("""
            select use, description
              from warehouse.identifier_set_use
             order by use
            """)

        set_uses = list(cursor)

    # Line up names nicely into a column
    template = "{:<%d}" % (max(len(su.use) for su in set_uses) + 3)

    for set_use in set_uses:
        click.secho(template.format(set_use.use), bold = True, nl = False)
        click.echo(set_use.description)

@set_use_.command("create")
@click.argument("use", metavar="<use>")
@click.argument("description", metavar = "<description>")

def set_use_create(use, description):
    """
    Create a new identifier set use.

    \b
    <use> is the name of the new use.
    <description> is a comment explaining the purpose of the use.
    """
    session = DatabaseSession()

    with session:
        use, = session.fetch_row("""
            insert into warehouse.identifier_set_use (use, description)
            values (%s, %s)
            returning use
            """, (use, description))

    click.echo(
        "Created identifier set use " +
        click.style(use, bold = True))
