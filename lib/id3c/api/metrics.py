"""
Web API metrics.
"""
import os
from prometheus_client import CollectorRegistry, GCCollector, PlatformCollector, ProcessCollector
import prometheus_flask_exporter
import prometheus_flask_exporter.multiprocess

from ..metrics import MultiProcessWriter


if "prometheus_multiproc_dir" in os.environ:
    FlaskMetrics = prometheus_flask_exporter.multiprocess.MultiprocessPrometheusMetrics
    MULTIPROCESS = True

else:
    FlaskMetrics = prometheus_flask_exporter.PrometheusMetrics
    MULTIPROCESS = False


# This instance is used by both our routes and create_app().
XXX = FlaskMetrics(
    app = None,
    path = None,
    defaults_prefix = prometheus_flask_exporter.NO_PREFIX,
    default_latency_as_histogram = False)


def register_app(app):
    XXX.init_app(app)

    # XXX TODO FIXME needs to be postfork for a pre-forking server like uWSGI
    if MULTIPROCESS:
        registry = CollectorRegistry(auto_describe = True)

        ProcessCollector(registry = registry)
        PlatformCollector(registry = registry)
        GCCollector(registry = registry)

        writer = MultiProcessWriter(registry)
        writer.start()
