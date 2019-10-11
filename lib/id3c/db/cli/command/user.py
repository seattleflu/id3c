"""
List and create users.
"""
import click
import logging
import id3c.db as db
from operator import attrgetter
from typing import List
from id3c.db.session import DatabaseSession
from id3c.db.cli import cli


LOG = logging.getLogger(__name__)


@cli.group("user", help = __doc__)
def user():
    pass


# Create subcommand
@user.command("create")
@click.argument("name", metavar = "<username>")

@click.option("--generate-password",
    help = "Assign a randomly generated password to the new user; "
           "it will be displayed on stdout after creation.",
    is_flag = True,
    flag_value = True)

@click.option("--role", "roles",
    metavar = "<name>",
    help = "Grant the named role to the new user",
    multiple = True)

@click.option("--comment",
    metavar = "<text>",
    help = "Description of the new user")

def create(name, *, generate_password: bool, roles: List, comment: str):
    """
    Create a new user.

    <username> is the login name of the new user.
    """
    session = DatabaseSession()

    with session:
        db.create_user(session, name, comment)
        db.grant_roles(session, name, roles)

        if generate_password:
            new_password = db.reset_password(session, name)

            click.echo(
                click.style("Password is ", bold = True) +
                click.style(new_password, fg = "red"))


# Reset password subcommand
@user.command("reset-password")
@click.argument("name", metavar = "<username>")

def reset_password(name):
    """
    Reset a user's password.

    <username> is the login name of the new user.

    The newly generated random password will be displayed on stdout.
    """
    session = DatabaseSession()

    with session:
        new_password = db.reset_password(session, name)

        click.echo(
            click.style("New password is ", bold = True) +
            click.style(new_password, fg = "red"))


# List subcommand
@user.command("ls")
def ls():
    """List users."""
    session = DatabaseSession()

    with session.cursor() as cursor:
        cursor.execute("""
            select usename as name,
                   pg_catalog.shobj_description(usesysid, 'pg_authid') as description,
                   coalesce(array_agg(groname order by groname) filter (where groname is not null), '{}') as roles
              from pg_catalog.pg_user
              left join pg_catalog.pg_group on (grolist @> array[usesysid])
             where usename not in ('postgres', 'rdsadmin')
             group by name, usesysid
             order by name
            """)

        users = list(cursor)

    # Line up name + description nicely into a column
    def maxlen(attr):
        return max(map(len, filter(None, map(attrgetter(attr), users)))) or 0

    template = "{:<%d}" % (maxlen("name") + 3)

    for user in users:
        click.secho(template.format(user.name), bold = True, nl = False)
        click.echo(", ".join(user.roles))
