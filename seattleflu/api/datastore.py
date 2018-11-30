"""
Datastore abstraction for our database.
"""
import logging
import psycopg2
from flask import jsonify
from psycopg2 import DataError
from werkzeug.exceptions import BadRequest
from .utils import export


LOG = logging.getLogger(__name__)

# Connection details are controlled entirely by standard libpq environment
# variables <https://www.postgresql.org/docs/current/libpq-envars.html>.
LOG.debug("Connecting to PostgreSQL database")

CONNECTION = psycopg2.connect("postgresql://")


@export
def store_enrollment(document: str):
    """
    Store the given enrollment JSON *document* (a **string**) in the backing
    database.

    Raises a :class:`BadRequestDataError` exception if the given *document*
    isn't valid.
    """
    with CONNECTION, CONNECTION.cursor() as cursor:
        try:
            cursor.execute(
                "INSERT INTO staging.enrollment (document) VALUES (%s)",
                    (document,))
        except DataError as error:
            raise BadRequestDataError(error) from None


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
