from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Optional
import os, traceback, abc
from flask import Flask, Response, Blueprint, render_template
import json, logging, importlib
from functools import partial
from stratus.util.config import Config, StratusLogger
from stratus_endpoint.handler.base import Task, Status
from flask_sqlalchemy import SQLAlchemy
from stratus.handlers.app import StratusCore
from jsonschema import validate

class RestAPIBase:
    __metaclass__ = abc.ABCMeta

    def __init__( self, name: str, core: StratusCore, **kwargs ):
        self.logger = StratusLogger.getLogger()
        self.parms = kwargs
        self.name = name
        self.core = core
        self.tasks = {}

    def addTask( self, task: Task ):
        self.tasks[ task.sid ] = task
        return task.sid

    def getStatus( self, cid: str = None ) -> Dict[str,Status]:
        statusMap = { sid: task.status() for sid, task in self.tasks.items() if ( cid is None or task.cid == cid ) }
        for sid, status in statusMap.items():
            if status in [ Status.ERROR, Status.COMPLETED ]:  del self.tasks[sid]
        return { sid: str(status) for sid, status in statusMap.items() }

    @abc.abstractmethod
    def _createBlueprint( self, app: Flask ): pass

    def instantiate( self, app: Flask ):
        bp: Blueprint = self._createBlueprint( app )
        self.logger.info( f"Instantiating API: {bp.name}" )
        app.register_blueprint( bp )

    def jsonResponse(self, response: Dict, code: int = 200 ) -> Response:
        return Response( response=json.dumps( response ), status=code, mimetype="application/json")

    def jsonRequest(self, requestSpec: str, schema: Dict = None ) -> Dict:
        try:
            requestDict = json.loads(requestSpec)
            if schema is not None: validate( instance=requestDict, schema=schema )
            requestDict["status"] = "submitted"
            return requestDict
        except Exception as err:
            return dict( status="error", message=f"Error parsing/validating request: '{requestSpec}'", error=str(err) )

class StratusApp(StratusCore):

    def __init__( self, **kwargs ):
        self.apis = []
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
        apiList = self.parm("API","core").split(",")
        for apiName in apiList:
            try:
                package_name = f"stratus.handlers.rest.api.{apiName}.app"
                module = importlib.import_module( package_name )
                constructor = getattr( module, "RestAPI" )
                rest_api: RestAPIBase = constructor( apiName, self )
                rest_api.instantiate( app )
                self.apis.append( rest_api )
            except Exception as err:
                self.logger.error( f"Error instantiating api {apiName}: {str(err)}")

    @staticmethod
    def render_server_error( ex: Exception ):
        print( str( ex ) )
        traceback.print_exc()
        return Response(response=json.dumps({ 'message': getattr(ex, 'message', repr(ex)), "code": 500, "id": "", "status": "error" } ), status=500, mimetype="application/json")

if __name__ == "__main__":
    HERE = os.path.dirname(os.path.abspath(__file__))
    SETTINGS_FILE = os.path.join(HERE, "server_test_settings.ini")
    app = StratusApp( settings=SETTINGS_FILE )
    app.run()
