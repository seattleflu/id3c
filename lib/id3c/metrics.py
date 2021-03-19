"""
Metrics handling functions.
"""
import logging
from prometheus_client.core import GaugeMetricFamily
from psycopg2.errors import InsufficientPrivilege
from .db import DatabaseSession


LOG = logging.getLogger(__name__)


class DatabaseCollector:
    """
    Collects metrics from the database, using an existing *session*.
    """
    def __init__(self, session: DatabaseSession):
        self.session = session


    def collect(self):
        with self.session:
            yield from self.estimated_row_total()


    def estimated_row_total(self):
        family = GaugeMetricFamily(
            "id3c_estimated_row_total",
            "Estimated number of rows in an ID3C database table",
            labels = ("schema", "table"))

        try:
            metrics = self.session.fetch_all(
                """
                select
                    ns.nspname          as schema,
                    c.relname           as table,
                    c.reltuples::bigint as estimated_row_count
                from
                    pg_catalog.pg_class c
                    join pg_catalog.pg_namespace ns on (c.relnamespace = ns.oid)
                where
                    ns.nspname in ('receiving', 'warehouse') and
                    c.relkind = 'r'
                order by
                    schema,
                    "table"
                """)
        except InsufficientPrivilege as error:
            LOG.error(f"Permission denied when collecting id3c_estimated_row_total metrics: {error}")
            return

        for metric in metrics:
            family.add_metric((metric.schema, metric.table), metric.estimated_row_count)

        yield family
