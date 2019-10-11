"""
A command-line interface to the Infectious Disease Data Distribution Center.
"""
import click
from typing import NoReturn


# Base command for all other commands
@click.group(help = __doc__)
def cli() -> NoReturn:
    pass


# Load all top-level id3c.db.cli.command packages, giving them an
# opportunity to register their commands using @cli.command(…).
from .command import *
