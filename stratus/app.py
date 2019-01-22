import os
# from stratus.views import blueprints
from connexion.resolver import RestyResolver
# from stratus.views.blueprints import SwaggerBlueprint, JsonBlueprint
from connexion.resolver import Resolver
from connexion.operations import AbstractOperation

import os, traceback
from flask import Flask, Response
import connexion, json, logging
from stratus.util.config import Config, StratusLogger, STRATUS_CONFIG
logger = StratusLogger.getLogger()
_HERE = os.path.dirname(__file__)
_SETTINGS = os.path.join(_HERE, 'settings.ini')

def render_server_error( ex: Exception ):
    print( str( ex ) )
    traceback.print_exc()
    return Response(response=json.dumps({ 'message': getattr(ex, 'message', repr(ex)), "code": 500 } ), status=500, mimetype="application/json")

class StratusResolver(Resolver):

    def __init__(self, default_handler_package: str ):
        Resolver.__init__(self)
        self.default_handler_package = default_handler_package

    def resolve_operation_id(self, operation: AbstractOperation):
        operation_id = operation.operation_id
        router_controller = operation.router_controller if operation.router_controller else self.default_handler_package
        return '{}.{}'.format(router_controller, operation_id)

app = connexion.FlaskApp("stratus", specification_dir='api/', debug=True )
app.add_error_handler( 500, render_server_error )
flask: Flask = app.app
flask.register_error_handler( TypeError, render_server_error )

settings = os.environ.get('FLASK_SETTINGS', _SETTINGS)
config_file = Config(settings)
app.app.config.update( config_file.get_map('flask') )
stratus_parms = config_file.get_map('stratus')
api = stratus_parms.get( 'API', None )
if api is None: raise Exception( "Missing required stratus parameter in settings.ini: 'API'" )   #  'hpda1.yaml
handler = stratus_parms['HANDLER']
if handler is None: raise Exception( "Missing required stratus parameter in settings.ini: 'HANDLER'" ) # 'stratus.handlers.hpda1'

# if blueprints is not None:
#     for bp in blueprints:
#         app.app.register_blueprint(bp)
#         print( "Register blueprint: " + str(bp))



app.add_api( api + ".yaml", resolver=StratusResolver( handler ) )
app.run( 5000 )