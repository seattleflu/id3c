"""
A command-line interface for the Seattle Flu Study database.
"""
import click
from typing import NoReturn


# Invoked by bin/db
@click.group(help = __doc__)
def cli() -> NoReturn:
    pass


# Load all top-level seattleflu.db.cli.command packages, giving them an
# opportunity to register their commands using @cli.command(…).
from .command import *


# Invoked via `python -m seattleflu.db.cli`.
if __name__ == "__main__":
    cli(prog_name = "db")
