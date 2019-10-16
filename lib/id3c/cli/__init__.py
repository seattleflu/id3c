"""
A command-line interface to the Infectious Disease Data Distribution Center.
"""
import click
import logging
import pkg_resources
from typing import NoReturn


LOG = logging.getLogger(__name__)


# Base command for all other commands
@click.group(help = __doc__)
def cli() -> NoReturn:
    pass


# Load all top-level id3c.cli.command packages, giving them an
# opportunity to register their commands using @cli.command(…).
from .command import *

# Load all extra commands from extensions.
for extension in pkg_resources.iter_entry_points("id3c.cli.commands"):
    if extension.dist:
        dist = f"{extension.dist.project_name} in {extension.dist.location}"
    else:
        dist = "unknown"

    LOG.debug(f"Loading commands from extension {extension!s} (distribution {dist})")
    extension.load()
