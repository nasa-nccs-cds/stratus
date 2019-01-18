import os
# from stratus.views import blueprints
from connexion.resolver import RestyResolver
# from stratus.views.blueprints import SwaggerBlueprint, JsonBlueprint

import os
from flask import Flask, Response
import connexion, json
from stratus.util.config import Config

_HERE = os.path.dirname(__file__)
_SETTINGS = os.path.join(_HERE, 'settings.ini')

def render_exception( ex: Exception ):
    return Response(response=json.dumps({ 'error': str(ex), 'message': getattr(ex, 'message', repr(ex))} ), status=401, mimetype="application/json")

app = connexion.FlaskApp("stratus", specification_dir='api/', debug=True )
app.add_error_handler( 500, render_exception )

settings = os.environ.get('FLASK_SETTINGS', _SETTINGS)
if settings is not None:
    config_file = Config(settings)
    app.app.config.update( config_file.get_map('flask') )

# if blueprints is not None:
#     for bp in blueprints:
#         app.app.register_blueprint(bp)
#         print( "Register blueprint: " + str(bp))

app.add_api( 'hpda1.yaml' ) #  , resolver=RestyResolver('stratus.handlers') )

app.run( 5000 )