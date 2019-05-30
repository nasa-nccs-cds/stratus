from connexion.resolver import Resolver
from connexion.operations import AbstractOperation
import traceback
from flask import Response
import connexion, json
from functools import partial
from flask_sqlalchemy import SQLAlchemy
from stratus.app.core import StratusCore
from stratus.app.base import StratusServerApp

class StratusResolver(Resolver):

    def __init__(self, api: str, stratus: StratusCore ):
        Resolver.__init__( self, self.function_resolver )
        self.api = api
        self.stratus = stratus

    def resolve_operation_id( self, operation: AbstractOperation ) -> str:
        return operation.operation_id

    def function_resolver( self, operation_id: str ) :
        clients = self.stratus.getClients()
        assert len(clients), "No handlers found for operation: " + operation_id
        return partial( clients[0].request, operation_id )

class StratusApp(StratusServerApp):

    def __init__( self, core: StratusCore ):
        StratusServerApp.__init__(self, core)
        self.app = connexion.FlaskApp("stratus.handlers.openapi", specification_dir='api/', debug=True )
        self.app.add_error_handler( 500, self.render_server_error )
        self.app.app.register_error_handler( TypeError, self.render_server_error )
        self.flask_parms = self.getConfigParms('flask')
        self.flask_parms[ 'SQLALCHEMY_DATABASE_URI' ] = self.flask_parms['DATABASE_URI']
        self.app.app.config.update( self.flask_parms )

        api = self.parm( 'API' )
        self.db = SQLAlchemy( self.app.app )
        self.app.add_api( api + ".yaml", resolver=StratusResolver( api, self.core ) )

    def run(self):
        port = self.flask_parms.get( 'PORT', 5000 )
        host = self.flask_parms.get('HOST', "127.0.0.1" )
        self.db.create_all( )
        return self.app.run( port=int( port ), host=host, debug=False )

    @staticmethod
    def render_server_error( ex: Exception ):
        print( str( ex ) )
        traceback.print_exc()
        return Response(response=json.dumps({ 'message': getattr(ex, 'message', repr(ex)), "code": 500, "rid": "", "status": "error" } ), status=500, mimetype="application/json")

