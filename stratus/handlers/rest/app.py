from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Optional
from stratus.handlers.client import StratusClient
import os, traceback
from flask import Flask, Response
import json, logging
from functools import partial
from stratus.util.config import Config, StratusLogger
from flask_sqlalchemy import SQLAlchemy
from stratus.handlers.app import StratusCore

class StratusApp(StratusCore):

    def __init__( self, **kwargs ):
        StratusCore.__init__( self, **kwargs )
        self.flask_parms = self.getConfigParms('flask')
        self.flask_parms['SQLALCHEMY_DATABASE_URI'] = self.flask_parms['DATABASE_URI']
        self.app = self.create_app( self.flask_parms )
        self.db = SQLAlchemy( self.app )
        try:            os.makedirs(self.app.instance_path)
        except OSError: pass

    def run(self):
        port = self.flask_parms.get( 'PORT', 5000 )
        host = self.flask_parms.get( 'HOST', "127.0.0.1" )
        self.db.create_all( )
        return self.app.run( port=int( port ), host=host, debug=False )

    def create_app(self, parms: Dict ):
        app = Flask( "stratus", instance_relative_config=True )
        app.config.from_mapping( parms )
        app.register_error_handler( TypeError, self.render_server_error )
        return app

    @staticmethod
    def render_server_error( ex: Exception ):
        print( str( ex ) )
        traceback.print_exc()
        return Response(response=json.dumps({ 'message': getattr(ex, 'message', repr(ex)), "code": 500, "id": "", "status": "error" } ), status=500, mimetype="application/json")

if __name__ == "__main__":
    app = StratusApp()
    app.run()
