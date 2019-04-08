"""
Run ETL routines
"""
import click
from seattleflu.db.cli.__main__ import cli


@cli.group("etl", help = __doc__)
def etl():
    pass


# Load all ETL subcommands.
__all__ = [
    "enrollments",
]

from . import *