from typing import Dict
import os, traceback, abc
from flask import Flask, Response, Blueprint, request
import json, importlib
from stratus_endpoint.util.config import StratusLogger
from stratus.app.core import StratusCore
from stratus.app.operations import WorkflowExeFuture
from stratus_endpoint.handler.base import TaskHandle, Status
from flask_sqlalchemy import SQLAlchemy
from stratus.app.base import StratusAppBase
from jsonschema import validate
HERE = os.path.dirname(os.path.abspath(__file__))
API_DIR = os.path.join( HERE, "api" )

class RestAPIBase:
    __metaclass__ = abc.ABCMeta

    def __init__( self, name: str, app: StratusAppBase, **kwargs ):
        self.logger = StratusLogger.getLogger()
        self.parms = kwargs
        self.name = name
        self.app = app
        self.tasks: Dict[str, TaskHandle] = {}

    def addTask( self, task: TaskHandle ):
        self.logger.info(f"REST-SERVER: Add Task {task.rid}" )
        self.tasks[ task.rid ] = task
        return task.rid

    def removeTask( self, rid: str ):
        self.logger.info(f"REST-SERVER: Delete Task {rid}")
        if rid in self.tasks:
            del self.tasks[ rid ]

    def getStatus( self, cid: str = None ) -> Dict[str,Status]:
        statusMap = { rid: task.status() for rid, task in self.tasks.items() if ( cid is None or task.cid == cid ) }
        result =  { rid: str(status) for rid, status in statusMap.items() }
        self.logger.info( f"REST-SERVER: getStatus(cid={cid}): {str(result)}, all tasks: " + str( [str(task) for task in self.tasks.values()] ))
        return result

    def getParameter(self, name: str, default = None, required = True ):
        param = request.args.get( name, default )
        assert required is False or param is not None, f"Missing required parameter: '{name}'"
        return param

    def _blueprint( self, app: Flask ):
        bp = Blueprint( self.name, __name__, url_prefix=f'/{self.name}' )
        self.logger.info( f"Instantiating BP: {bp.name}" )
        app.register_blueprint( bp )
        return bp

    @abc.abstractmethod
    def _addRoutes(self, bp: Blueprint):
        self.logger(" Error, no routes declated")

    def instantiate( self, app: Flask ):
        bp = Blueprint( self.name, __name__, url_prefix=f'/{self.name}' )
        self.logger.info( f"Instantiating API--> {bp.name}, class = { str(self.__class__ ) }" )
        self._addRoutes(bp)
        app.register_blueprint( bp )
        self.logger.info( "URL MAP: \n" + str( app.url_map ) )

    def jsonResponse( self, response: Dict, code: int = 200 ) -> Response:
        return Response( response=json.dumps( response ), status=code, mimetype="application/json")

    def jsonRequest(self, requestSpec: str, schema: Dict = None ) -> Dict:
        try:
            requestDict = json.loads(requestSpec)
            if schema is not None: validate( instance=requestDict, schema=schema )
            requestDict["status"] = "submitted"
            return requestDict
        except Exception as err:
            return dict( status="error", message=f"Error parsing/validating request: '{requestSpec}'", error=str(err) )

    def xmlResponse(self, type: str, message: Dict, code: int = 200 ) -> Response:
        return Response( response="" , status=code, mimetype="application/xml")

class StratusApp(StratusAppBase):

    def __init__( self, core: StratusCore ):
        self.apis = []
        self.available_apis =  [f.name for f in os.scandir(API_DIR) if f.is_dir() ]
        StratusAppBase.__init__( self, core )
        self.flask_parms = self.getConfigParms('flask')
        self.flask_parms['SQLALCHEMY_DATABASE_URI'] = self.flask_parms.get('DATABASE_URI','sqlite:////tmp/test.db')
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

    def addApis( self, app ):
        apiListParm = self.core.parms.get("API",None)
        apiList = self.available_apis if apiListParm is None else apiListParm.split(",")
        for apiName in self.available_apis:
            if apiName in apiList:
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
        return Response(response=json.dumps({ 'message': getattr(ex, 'message', repr(ex)), "code": 500, "rid": "", "status": "error" } ), status=500, mimetype="application/json")

if __name__ == "__main__":
    HERE = os.path.dirname(os.path.abspath(__file__))
    SETTINGS_FILE = os.path.join(HERE, "wps_server_test_settings.ini")
    core = StratusCore( SETTINGS_FILE  )
    app = core.getApplication()
    app.run()
