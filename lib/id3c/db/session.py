"""
Database session creation and management.
"""
import logging
import os
import psycopg2
import psycopg2.extensions
from contextlib import contextmanager
from psycopg2 import DatabaseError
from psycopg2.extras import NamedTupleCursor
from psycopg2.sql import SQL, Identifier
from typing import Any, Iterable, Iterator, Mapping, Tuple, Union
from uuid import uuid4
from ..cli.utils import running_command_name
from ..utils import shorten


LOG = logging.getLogger(__name__)


class DatabaseSession:
    connection: psycopg2.extensions.connection

    def __init__(self, *, username: str = None, password: str = None) -> None:
        """
        Connects to the database.

        Connection details like host and database are controlled entirely by
        `standard libpq environment variables`__.

        __ https://www.postgresql.org/docs/current/libpq-envars.html

        Credentials are provided by either

        a. the environment, using PGUSER or a PGSERVICE definition and
           PGPASSWORD or a PGPASSFILE, or

        b. the optional keyword parameters *username* and *password*.

        Keyword parameters override credentials from the environment.
        """
        LOG.debug(f"Authenticating to PostgreSQL database using {pg_environment()}")

        connect_params = {
            "cursor_factory": NamedTupleCursor,
            "fallback_application_name": fallback_application_name(),

            **({"user": username}     if username is not None else {}),
            **({"password": password} if password is not None else {}),
        }

        try:
            # connect() requires a DSN as the first arg even if the connection
            # details are fully-specified by the environment, but we don't need to
            # fill it with anything.
            self.connection = psycopg2.connect("", **connect_params)
        except DatabaseError as error:
            LOG.error(f"Authentication failed: {error}")
            raise error from None

        LOG.info(f"Connected to {self.session_info()}")


    @property
    def __enter__(self):
        """Proxy for the underlying connection's ``__enter__`` method."""
        return self.connection.__enter__

    @property
    def __exit__(self):
        """Proxy for the underlying connection's ``__exit__`` method."""
        return self.connection.__exit__

    @property
    def cursor(self) -> NamedTupleCursor:
        """Proxy for the underlying connection's ``cursor`` method."""
        return self.connection.cursor

    @property
    def commit(self):
        """Proxy for the underlying connection's ``commit`` method."""
        return self.connection.commit

    @property
    def rollback(self):
        """Proxy for the underlying connection's ``rollback`` method."""
        return self.connection.rollback


    @contextmanager
    def savepoint(self, name: str = None) -> Iterator:
        """
        Context manager for database savepoints.

        >>> with db.savepoint(): # doctest: +SKIP
        ...     ... # execute database statements here

        A savepoint is created with *name* (or a random id if *name* is
        ``None``) when the ``with`` block is entered.  If the ``with``
        block returns normally, the savepoint is released and changes
        are persisted to the transaction (but the transaction is not
        committed).  If the ``with`` block returns an exception, the
        transaction is rolled back to the savepoint.
        """
        if name is None:
            name = str(uuid4())

        # Quote the name as a SQL identifier
        id = Identifier(name)

        with self.cursor() as cursor:
            LOG.debug(f"Creating savepoint {name}")

            cursor.execute(
                SQL("savepoint {}").format(id))

            try:
                yield

            except Exception as error:
                LOG.debug(f"Rolling back to savepoint {name}")

                cursor.execute(
                    SQL("rollback to savepoint {}").format(id))

                raise error from None

            else:
                LOG.debug(f"Releasing savepoint {name}")

                cursor.execute(
                    SQL("release savepoint {}").format(id))


    def fetch_row(self, sql: str, values: Union[Tuple, Mapping] = None) -> Any:
        """
        Fetches the first row from the results of the *sql* query.

        Most useful for queries which only return a single row, such as selects on
        a unique key or ``insert ... returning`` statements for a single record.
        """
        with self.cursor() as cursor:
            cursor.execute(sql, values)
            assert cursor.rowcount <= 1, f"More than one result row for fetch_row({sql!r}, {values!r})"
            return cursor.fetchone()


    def fetch_all(self, sql: str, values: Union[Tuple, Mapping] = None) -> Any:
        """
        Fetches all rows from the results of the *sql* query. Useful for
        writing alerts for new rows in queries of interest.
        """
        with self.cursor() as cursor:
            cursor.execute(sql, values)
            return cursor.fetchall()


    def copy_from_ndjson(self, qualified_column: Tuple, stream) -> int:
        """
        Copies JSON documents (one per line) from the file-like object *stream*
        into *qualified_column*.

        *qualified_column* should be a tuple of (table, column) or (schema,
        table, column).  All identifiers will be properly quoted by this
        method.
        """
        assert len(qualified_column) >= 2, \
            "No table name included in qualified column tuple"

        table  = SQL(".").join(map(Identifier, qualified_column[0:-1]))
        column = Identifier(qualified_column[-1])

        with self.cursor() as cursor:
            # The text format supported by COPY allows certain escape sequences
            # in the data.  To avoid that and thus avoid the need to
            # double-escape our JSON values, use the CSV format configured
            # with ASCII control characters which aren't permitted literals in
            # JSON as the delimiter and quote character.
            cursor.copy_expert(
                SQL("""
                    copy {} ({}) from stdin with (
                        format csv,
                        delimiter E'\\x1f',
                        quote E'\\x1b',
                        encoding 'utf-8')
                    """).format(table, column),
                stream)

            return cursor.rowcount


    def session_info(self) -> str:
        """
        Describes the current database session using a concise string.
        """
        info = [
            "user",
            "dbname",
            "host",
            "port",
            "sslmode",
        ]

        params = self.connection.get_dsn_parameters()

        return " ".join(
            f"{param}={params.get(param)}"
                for param in info
                 if params.get(param))


def pg_environment() -> dict:
    """
    Returns a dictionary of environment variables starting with ``PG``.

    Masks the value of `PGPASSWORD`, if present.
    """
    env = {
        key: value
            for key, value in os.environ.items()
             if key.startswith("PG")
    }

    if "PGPASSWORD" in env:
        env["PGPASSWORD"] = "***MASKED***"

    return env


def fallback_application_name() -> str:
    """
    Returns the application name to use for the database connection when
    ``PGAPPNAME`` isn't set.

    Constructed based on the current running CLI command, else ``id3c``.
    """
    # "The application_name can be any string of less than NAMEDATALEN
    # characters (64 characters in a standard build)."¹
    #
    # psycopg2 / libpq will truncate for us, but they will issue a NOTICE log
    # message if they do.  Avoid the cluttery notice by truncating ourselves.
    #
    # ¹ https://www.postgresql.org/docs/current/runtime-config-logging.html#GUC-APPLICATION-NAME
    max_len = 64
    appname = running_command_name()

    return shorten(appname, max_len, "...")
