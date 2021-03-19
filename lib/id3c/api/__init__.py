"""
Web API
"""
import logging
from flask import Flask
from . import config, metrics
from .routes import blueprints


LOG = logging.getLogger(__name__)


def create_app():
    app = Flask(__name__)
    app.config.update(config.from_environ())

    for blueprint in blueprints:
        app.register_blueprint(blueprint)

    metrics.register_app(app)

    LOG.debug(f"app root is {app.root_path}")
    LOG.debug(f"app static directory is {app.static_folder}")

    return app
