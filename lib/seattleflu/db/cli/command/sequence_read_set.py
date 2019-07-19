"""
Parse and upload sequence read sets.

Sequence read sets contain a sample and urls of an external datastore for a set
of FASTQ files from a single sequencing run.
"""
import click
import logging
import glob
import json
from seattleflu.db.cli import cli
from seattleflu.db.session import DatabaseSession
from seattleflu.db.datatypes import as_json


LOG = logging.getLogger(__name__)


@cli.group("sequence-read-set", help=__doc__)
def sequence_read_set():
    pass

@sequence_read_set.command("parse")
@click.argument("fastq_directory", metavar = "<FASTQ directory>")

def parse(fastq_directory):
    """
    Find all fastq.gz files within a provided *directory* and group them by
    samples into sequence read sets.

    All sequence read sets are output to stdout as newline-delimited JSON
    records.  You will likely want to redirect stdout to a file.
    """
    sequence_read_sets = {}
    for filename in glob.glob(fastq_directory + "/*.fastq.gz"):
        sample = filename.split('/')[-1].split('_')[0]
        if not sequence_read_sets.get(sample):
            sequence_read_sets[sample] = [filename]
        else:
            sequence_read_sets[sample].append(filename)

    for sample in sequence_read_sets:
        print(as_json({"sample": sample, "urls": sequence_read_sets[sample]}))


@sequence_read_set.command("upload")
@click.argument("sequence-read-set-file",
    metavar = "<sequence-read-set.ndjson>",
    type = click.File("r"))
@click.argument("unknown-sample-output",
    metavar= "<unknown-sample-output.ndjson>",
    type=click.File("w"))
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

def upload(sequence_read_set_file, unknown_sample_output, action: str):
    """
    Upload sequence read sets into the database warehouse.

    <sequence-read-set.ndjson> must be a newline delimited JSON file produced
    by this command's sibling command.

    Sequence read sets with NWGC sample IDs that cannot be found within the
    database warehouse are printed out as newline delimited JSON file
    <unknown-sample-output.ndjson>.
    """

    db = DatabaseSession()
    processed_without_error = None
    try:
        for sequence_read_set in sequence_read_set_file:
            sample_set = json.loads(sequence_read_set)
            nwgc_id = sample_set.get("sample")
            urls = sample_set.get("urls")
            with db.savepoint(f"sequence read set {nwgc_id}"):
                LOG.info(f"Processing sequence read set for sample {nwgc_id}")
                sample_id = find_sample(db, nwgc_id)
                if sample_id is None:
                    unknown_sample_output.write(sequence_read_set)
                    continue
                sequence_read_set = insert_sequence_read_set(db, sample_id, urls)
                LOG.info(f"Finished uploading sequence read set for sample {nwgc_id}")
    except Exception as error:
        processed_without_error = False
        LOG.error(f"Aborting with error: {error}")
        raise error from None
    else:
        processed_without_error = True
    finally:
        if action == "prompt":
            ask_to_commit = \
                "Commit all changes?" if processed_without_error else \
                "Commit successfully processed manifest records up to this point?"

            commit = click.confirm(ask_to_commit)
        else:
            commit = action == "commit"

        if commit:
            LOG.info(
                "Committing all changes" if processed_without_error else \
                "Committing successfully processed manifest records up to this point")
            db.commit()

        else:
            LOG.info("Rolling back all changes; the database will not be modified")
            db.rollback()


def find_sample(db: DatabaseSession, nwgc_id: str) -> int:
    """
    Find sample within warehouse that has *nwgc_id* in the sample details.
    """
    LOG.debug(f"Looking up sample with NWGC ID: «{nwgc_id}»")

    sample = db.fetch_row("""
        select sample_id as id
          from warehouse.sample
         where details ->> 'nwgc_id' = %s
        """, (nwgc_id,))

    if not sample:
        LOG.error(f"No sample with NWGC ID «{nwgc_id}» found")
        return None

    LOG.info(f"Found sample {sample.id}")
    return sample.id


def insert_sequence_read_set(db: DatabaseSession, sample_id: int, urls: list):
    """
    Insert sequencing read set directly into warehouse.sequence_read_set,
    with the *sample_id* and *urls*.
    """
    LOG.debug(f"Inserting sequence read set for sample {sample_id}")

    data = {
        "sample_id": sample_id,
        "urls": urls
    }

    sequence_read_set = db.fetch_row("""
        insert into warehouse.sequence_read_set (sample_id, urls)
            values (%(sample_id)s, %(urls)s)
        returning sequence_read_set_id as id
        """, data)
    assert sequence_read_set.id, "Insert failed"
    return sequence_read_set
