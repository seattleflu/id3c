"""
Parse and upload sequence read sets.

Sequence read sets contain a sample and urls of an external datastore for a set
of FASTQ files from a single sequencing run.
"""
import click
import logging
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, Optional
from urllib.parse import urljoin
from id3c.cli import cli
from id3c.db.session import DatabaseSession
from id3c.db.datatypes import as_json


LOG = logging.getLogger(__name__)


@cli.group("sequence-read-set", help=__doc__)
def sequence_read_set():
    pass

@sequence_read_set.command("parse")

@click.argument("fastq_directory",
    metavar = "<directory>",
    type = click.Path(exists=True, file_okay=False))

@click.option("--filename-pattern",
    help = "Regex pattern to match sample in expected filename",
    metavar = "<regex>",
    default = r'^(?P<sample>\d+)_',
    show_default = True)

@click.option("--url-prefix",
    help = "Base for fully-qualifying sequence read set URLs",
    metavar = "<url>",
    default = "file://rhino.fhcrc.org",
    show_default = True)

def parse(fastq_directory, filename_pattern, url_prefix):
    """
    Find all *.fastq.gz files within a provided <directory>, which should be an
    absolute file path.

    The provided --filename-pattern regular expression is used to extract the
    sample ID from each FASTQ filename.  The regex should contain a capture
    group named "sample".  Each set of files with the same sample ID are
    grouped into a single sequence read set.

    All sequence read sets are output to stdout as newline-delimited JSON
    records.  You will likely want to redirect stdout to a file.
    """
    sequence_read_sets: Dict[str, list] = defaultdict(list)
    filename_pattern = re.compile(filename_pattern)

    for filepath in list(Path(fastq_directory).glob("*.fastq.gz")):
        filename = filepath.name
        # Check the filename matches provided filename pattern
        filename_match = filename_pattern.match(filename)
        assert filename_match, f"Filename {filename} doesn't match provided --filename-pattern"

        # Extract the sample from the filename_match
        try:
            sample = filename_match.group("sample")
        except IndexError:
            LOG.error(f"Filename {filename} matched provided --filename-pattern, but didn't extract a «sample» capture group")
            raise

        sequence_read_sets[sample].append(urljoin(url_prefix, str(filepath)))

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
                    LOG.warning(f"Skipping sample with NWGC ID «{nwgc_id}» because it was not found within warehouse.")
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
                "Commit successfully processed sequence read set records up to this point?"

            commit = click.confirm(ask_to_commit)
        else:
            commit = action == "commit"

        if commit:
            LOG.info(
                "Committing all changes" if processed_without_error else \
                "Committing successfully processed sequence read set records up to this point")
            db.commit()

        else:
            LOG.info("Rolling back all changes; the database will not be modified")
            db.rollback()


def find_sample(db: DatabaseSession, nwgc_id: str) -> Optional[int]:
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
