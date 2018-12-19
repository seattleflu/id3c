"""
Seattle Flu Study informatics API
"""
import logging
import os
from flask import Flask
from . import config
from .router import blueprints


# Setup logging
handler = logging.StreamHandler()

handler.setFormatter(
    logging.Formatter(
        '[%(asctime)s] %(name)s %(levelname)s: %(message)s'))

LOG = logging.getLogger(__name__)
LOG.addHandler(handler)

LOG_LEVEL = os.environ.get("LOG_LEVEL")

if LOG_LEVEL:
    LOG.setLevel(LOG_LEVEL.upper())


def create_app():
    app = Flask(__name__)
    app.config.update(config.from_environ())

    for blueprint in blueprints:
        app.register_blueprint(blueprint)

    if app.debug:
        LOG.setLevel("DEBUG")

    LOG.debug(f"app root is {app.root_path}")
    LOG.debug(f"app static directory is {app.static_folder}")

    return app
