import os
from stratus.views import blueprints
from connexion.resolver import RestyResolver

import os
from flask import Flask
import connexion
from stratus.util.config import Config

_HERE = os.path.dirname(__file__)
_SETTINGS = os.path.join(_HERE, 'settings.ini')

app = connexion.FlaskApp("stratus", specification_dir='api/' )
app.add_api( 'hpda-1.0.yaml' ) #, resolver=RestyResolver('stratus.handlers.hpda') )

# load configuration
settings = os.environ.get('FLASK_SETTINGS', _SETTINGS)
if settings is not None:
    config_file = Config(settings)
    app.app.config.update( config_file.get_map('flask') )

# register blueprints
if blueprints is not None:
    for bp in blueprints:
        app.app.register_blueprint(bp)

app.run( 5000 )