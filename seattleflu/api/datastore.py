"""
Datastore abstraction for our database.
"""
import logging
import psycopg2
from flask import jsonify
from psycopg2 import DataError, DatabaseError, ProgrammingError
from typing import Any
from werkzeug.exceptions import BadRequest, Forbidden
from .exceptions import AuthenticationRequired
from .utils import export


LOG = logging.getLogger(__name__)

# Really psycopg2.extensions.connection, but avoiding annotating that so it
# isn't relied upon.
Session = Any


@export
def login(username: str, password: str) -> Session:
    """
    Creates a new database session authenticated as the given user.

    Returns an opaque session object which other functions in this module
    require.
    """
    # Connection details like host and database are controlled entirely by
    # standard libpq environment variables:
    #
    #    https://www.postgresql.org/docs/current/libpq-envars.html
    #
    LOG.debug(f"Authenticating to PostgreSQL database as '{username}'")

    try:
        session = psycopg2.connect(user = username, password = password)
    except DatabaseError as error:
        LOG.error(f"Authentication failed: {error}")
        raise AuthenticationRequired() from None

    LOG.debug(f"Session created for {session_info(session)}")

    return session


def session_info(session) -> str:
    """
    Takes a *session* object and returns a concise string describing it.
    """
    info = [
        "user",
        "dbname",
        "host",
        "port",
        "sslmode",
    ]

    params = session.get_dsn_parameters()

    return " ".join(
        f"{param}={params.get(param)}"
            for param in info
             if params.get(param))


@export
def store_enrollment(session: Session, document: str) -> None:
    """
    Store the given enrollment JSON *document* (a **string**) in the backing
    database using *session*.

    Raises a :class:`BadRequestDataError` exception if the given *document*
    isn't valid and a :class:`Forbidden` exception if the database reports a
    `permission denied` error.
    """
    with session, session.cursor() as cursor:
        try:
            cursor.execute(
                "INSERT INTO staging.enrollment (document) VALUES (%s)",
                    (document,))

        except DataError as error:
            raise BadRequestDataError(error) from None

        except ProgrammingError as error:
            if error.diag.message_primary.startswith("permission denied"):
                raise Forbidden()
            else:
                raise error from None


@export
class BadRequestDataError(BadRequest):
    """
    Subclass of :class:`werkzeug.exceptions.BadRequest` which takes a
    :class:`psycopg2.DataError` and forms a JSON response detailing the error.

    This intentionally does not expose the query context itself, only the
    context related to the data handling.
    """
    def __init__(self, error: DataError) -> None:
        super().__init__()

        LOG.error("BadRequestDataError: %s", error)

        self.response = jsonify({
            "error": error.diag.message_primary,
            "detail": error.diag.message_detail,
            "context": error.diag.context,
        })

        self.response.status_code = self.code
