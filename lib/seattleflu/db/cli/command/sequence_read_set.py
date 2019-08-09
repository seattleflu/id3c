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
from urllib.parse import urljoin
from seattleflu.db.cli import cli
from seattleflu.db.session import DatabaseSession
from seattleflu.db.datatypes import as_json


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
    sequence_read_sets = defaultdict(list)
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
