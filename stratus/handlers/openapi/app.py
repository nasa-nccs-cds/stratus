from connexion.resolver import Resolver
from connexion.operations import AbstractOperation
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Optional
from stratus.handlers.client import StratusClient
import os, traceback
from flask import Flask, Response
import connexion, json, logging
from functools import partial
from stratus.util.config import Config, StratusLogger
from flask_sqlalchemy import SQLAlchemy
from stratus.handlers.app import StratusCore

class StratusResolver(Resolver):

    def __init__(self, api: str  ):
        Resolver.__init__( self, self.function_resolver )
        self.api = api

    def resolve_operation_id( self, operation: AbstractOperation ) -> str:
        return operation.operation_id

    def function_resolver( self, operation_id: str ) :
        clients = StratusCore.getClients( operation_id )
        assert len(clients), "No handlers found for epa: " + operation_id
        return partial( clients[0].request, operation_id )

class StratusApp(StratusCore):

    def __init__(self):
        StratusCore.__init__(self)
        self.app = connexion.FlaskApp("stratus.handlers.openapi", specification_dir='api/', debug=True )
        self.app.add_error_handler( 500, self.render_server_error )
        self.app.app.register_error_handler( TypeError, self.render_server_error )
        self.flask_parms = self.getConfigParms('flask')
        self.flask_parms[ 'SQLALCHEMY_DATABASE_URI' ] = self.flask_parms['DATABASE_URI']
        self.app.app.config.update( self.flask_parms )
        self.parms = self.getConfigParms('stratus')
        api = self.getParameter( 'API' )
        self.db = SQLAlchemy( self.app.app )
        self.app.add_api( api + ".yaml", resolver=StratusResolver(api) )

    def run(self):
        port = self.flask_parms.get( 'PORT', 5000 )
        host = self.flask_parms.get('HOST', "127.0.0.1" )
        self.db.create_all( )
        return self.app.run( port=int( port ), host=host, debug=False )

    def getParameter(self, name: str, default = None ) -> str:
        parm = self.parms.get( name, default )
        if parm is None: raise Exception( "Missing required stratus parameter in settings.ini: " + name )
        return parm

    @staticmethod
    def render_server_error( ex: Exception ):
        print( str( ex ) )
        traceback.print_exc()
        return Response(response=json.dumps({ 'message': getattr(ex, 'message', repr(ex)), "code": 500, "id": "", "status": "error" } ), status=500, mimetype="application/json")


app = StratusApp()

if __name__ == "__main__":
    app.run()
