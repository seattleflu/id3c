"""
Metrics handling functions.
"""
import logging
import os
import threading
from prometheus_client import CollectorRegistry, REGISTRY as DEFAULT_REGISTRY
from prometheus_client.core import GaugeMetricFamily
from prometheus_client.values import ValueClass
from psycopg2.errors import InsufficientPrivilege
from time import sleep
from .db import DatabaseSession
from .utils import set_thread_name


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


class MultiProcessWriter(threading.Thread):
    def __init__(self, registry: CollectorRegistry = DEFAULT_REGISTRY, interval: int = 15):
        super().__init__(name = "metrics writer", daemon = True)

        self.registry = registry
        self.interval = interval

    def run(self):
        set_thread_name(self)

        while True:
            for metric in self.registry.collect():
                for sample in metric.samples:
                    if metric.type == "gauge":
                        # Metrics from GaugeMetricFamily will not have the
                        # attribute set, for example.
                        multiprocess_mode = getattr(metric, "_multiprocess_mode", "all")
                    else:
                        multiprocess_mode = ""

                    value = ValueClass(
                        metric.type,
                        metric.name,
                        sample.name,
                        sample.labels.keys(),
                        sample.labels.values(),
                        multiprocess_mode = multiprocess_mode)

                    value.set(sample.value)

            sleep(self.interval)
