import pytest
from id3c.cli import cli
from operator import attrgetter
from typing import Callable, NamedTuple

class Command(NamedTuple):
    name: str
    function: Callable

def walk_commands(name, command):
    yield Command(" ".join(name), command)

    try:
        subcommands = command.commands
    except AttributeError:
        pass
    else:
        for subname, subcommand in subcommands.items():
            yield from walk_commands([*name, subname], subcommand)

commands = list(walk_commands(["id3c"], cli))

@pytest.mark.parametrize("command", commands, ids = attrgetter("name"))
def test_help(command):
    with pytest.raises(SystemExit) as exit:
        command.function(["--help"])

    assert exit.value.code == 0, f"{command.name} exited with error"
