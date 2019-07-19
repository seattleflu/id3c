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
