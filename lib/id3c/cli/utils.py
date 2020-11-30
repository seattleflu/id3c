"""
CLI utilities.
"""
import click


def running_command_name() -> str:
    """
    Returns the current CLI command name as a space-separated string, or
    ``id3c`` if not running under any command.
    """
    appname = None
    context = click.get_current_context(silent = True)

    if context:
        appname = context.command_path

    if not appname:
        appname = "id3c"

    return appname
