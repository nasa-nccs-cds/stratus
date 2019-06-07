from typing import Dict
import os, traceback, abc
from flask import Flask, Response, Blueprint, request
import json, importlib
from stratus_endpoint.util.config import StratusLogger
from stratus.app.core import StratusCore
from stratus_endpoint.handler.base import TaskHandle, Status
from flask_sqlalchemy import SQLAlchemy
from stratus.app.base import StratusAppBase, StratusServerApp
from jsonschema import validate
from threading import Thread
HERE = os.path.dirname(os.path.abspath(__file__))
API_DIR = os.path.join( HERE, "api" )

class RestAPIBase:
    __metaclass__ = abc.ABCMeta

    def __init__(self, name: str, app: StratusAppBase, **kwargs):
        self.logger = StratusLogger.getLogger()
        self.parms = kwargs
        self.name =  name
        self.app: StratusAppBase = app

    def getStatus( self, rid: str ) -> Dict[str,str]:
        workflow = self.app.getWorkflow(rid)
        if workflow is None:
            if rid in self.app.registeredRequests:  return { "status": Status.str( Status.IDLE  ), "rid": rid }
            else:                                   return { "status": Status.str( Status.ERROR ), "rid": rid, "message": "Unknown request: " + rid }
        else:
            status = workflow.status()
            result = { "status": Status.str( status ), "rid": rid }
            if status == Status.ERROR:
                result["message"] = str( workflow.getResult().exception() )
            self.logger.info( f"REST-SERVER: getStatus(rid={rid}): {str(result)}, all tasks: {self.app.getWorkflows().keys()}" )
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

class FlaskThread(Thread):
    def __init__(self, parms ):
        Thread.__init__(self)
        self.port = parms.get( 'PORT', 5000 )
        self.host = parms.get( 'HOST', "127.0.0.1" )
        self.app = Flask( "stratus", instance_relative_config=True )
        self.app.config.from_mapping(parms)
        self.db = SQLAlchemy(self.app)
        try: os.makedirs(self.app.instance_path)
        except OSError: pass

    def run(self):
        self.db.create_all()
        self.app.run( port=int( self.port ), host=self.host, debug=False )

class StratusApp(StratusServerApp):

    def __init__( self, core: StratusCore, **kwargs ):
        self.apis = []
        self.available_apis =  [f.name for f in os.scandir(API_DIR) if f.is_dir() ]
        StratusServerApp.__init__(self, core, **kwargs)
        self.flask_parms = self.getConfigParms('flask')
        self.flask_parms['SQLALCHEMY_DATABASE_URI'] = self.flask_parms.get('DATABASE_URI','sqlite:////tmp/test.db')

    def initInteractions(self):
        self.flaskThread = FlaskThread( self.flask_parms  )
        self.addApis( self.flaskThread.app )
        self.flaskThread.app.register_error_handler(TypeError, self.render_server_error)
        self.flaskThread.start()

    def updateInteractions(self):
        pass

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
                    self.logger.error( f"Error instantiating api {apiName}: {str(err)}\n" + traceback.format_exc( ))

    @staticmethod
    def render_server_error( ex: Exception ):
        print( str( ex ) )
        traceback.print_exc()
        return Response(response=json.dumps({ 'message': getattr(ex, 'message', repr(ex)), "code": 500, "rid": "", "status": "error" } ), status=500, mimetype="application/json")

if __name__ == "__main__":
    HERE = os.path.dirname(os.path.abspath(__file__))
    SETTINGS_FILE = os.path.join(HERE, "wps_server_edas_settings.ini")
    core = StratusCore( SETTINGS_FILE  )
    app: StratusApp = core.getApplication()
    app.start()
