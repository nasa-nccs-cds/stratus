from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Optional
from stratus.handlers.client import StratusClient
import os, traceback, abc
from flask import Flask, Response, Blueprint, render_template
import json, logging, importlib
from functools import partial
from stratus.util.config import Config, StratusLogger
from flask_sqlalchemy import SQLAlchemy
from stratus.handlers.app import StratusCore

class RestAPIBase:
    __metaclass__ = abc.ABCMeta

    def __init__( self, name, **kwargs ):
        self.logger = StratusLogger.getLogger()
        self.parms = kwargs
        self.name = name

    @abc.abstractmethod
    def _createBlueprint( self ): pass

    def instantiate( self, app ):
        bp: Blueprint = self._createBlueprint()
        self.logger.info( f"Instantiating API: {bp.name}" )
        app.register_blueprint( bp )

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
        self.addApis( app )
        return app

    def addApis(self, app ):
        apiList = self.parm("API","stratus").split(",")
        for apiName in apiList:
            try:
                package_name = f"stratus.handlers.rest.{apiName}.app"
                module = importlib.import_module( package_name )
                constructor = getattr( module, "RestAPI" )
                rest_api: StratusRestAPI = constructor( apiName )
                rest_api.instantiate( app )
            except Exception as err:
                self.logger.error( f"Error instantiating api {apiName}: {str(err)}")

    @staticmethod
    def render_server_error( ex: Exception ):
        print( str( ex ) )
        traceback.print_exc()
        return Response(response=json.dumps({ 'message': getattr(ex, 'message', repr(ex)), "code": 500, "id": "", "status": "error" } ), status=500, mimetype="application/json")

if __name__ == "__main__":
    HERE = os.path.dirname(os.path.abspath(__file__))
    SETTINGS_FILE = os.path.join(HERE, "test_settings.ini")
    app = StratusApp( settings=SETTINGS_FILE )
    app.run()
