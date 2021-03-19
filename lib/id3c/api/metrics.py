"""
Web API metrics.
"""
import prometheus_flask_exporter
import prometheus_flask_exporter.multiprocess


if "prometheus_multiproc_dir" in os.environ:
    FlaskMetrics = prometheus_flask_exporter.multiprocess.MultiprocessPrometheusMetrics
else:
    FlaskMetrics = prometheus_flask_exporter.PrometheusMetrics


# This instance is used by both our routes and create_app().
metrics = FlaskMetrics(
    app = None,
    path = None,
    defaults_prefix = prometheus_flask_exporter.NO_PREFIX,
    default_latency_as_histogram = False)
