import os
from stratus.views import blueprints

import os
from flask import Flask
from stratus.util.config import Config

_HERE = os.path.dirname(__file__)
_SETTINGS = os.path.join(_HERE, 'settings.ini')

app = Flask("stratus")

# load configuration
settings = os.environ.get('FLASK_SETTINGS', _SETTINGS)
if settings is not None:
    app.config_file = Config(settings)
    app.config.update(app.config_file.get_map('flask'))

# register blueprints
if blueprints is not None:
    for bp in blueprints:
        app.register_blueprint(bp)

