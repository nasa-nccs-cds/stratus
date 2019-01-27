from connexion.resolver import Resolver
from connexion.operations import AbstractOperation
import os, traceback
from flask import Flask, Response
import connexion, json, logging
from stratus.util.config import Config, StratusLogger
from flask_sqlalchemy import SQLAlchemy
from celery import Celery

class StratusResolver(Resolver):

    def __init__(self, default_handler_package: str ):
        Resolver.__init__(self)
        self.default_handler_package = default_handler_package

    def resolve_operation_id(self, operation: AbstractOperation):
        operation_id = operation.operation_id
        router_controller = operation.router_controller if operation.router_controller else self.default_handler_package
        return '{}.{}'.format(router_controller, operation_id)

class StratusApp:
    HERE = os.path.dirname(__file__)
    SETTINGS = os.path.join( HERE, 'settings.ini')

    def __init__(self):
        self.logger = StratusLogger.getLogger()
        self.app = connexion.FlaskApp("stratus", specification_dir='api/', debug=True )
        self.app.add_error_handler( 500, self.render_server_error )
        self.app.app.register_error_handler( TypeError, self.render_server_error )
        settings = os.environ.get('FLASK_SETTINGS', self.SETTINGS )
        config_file = Config(settings)
        flask_parms = config_file.get_map('flask')
        flask_parms[ 'SQLALCHEMY_DATABASE_URI' ] = flask_parms['DATABASE_URI']
        self.app.app.config.update( flask_parms )
        self.parms = config_file.get_map('stratus')
        api = self.getParameter( 'API' )
        handler = self.getParameter( 'HANDLER' )
        self.celery = self.make_celery( self.app.app )
        self.db = SQLAlchemy( self.app.app )
        self.app.add_api( api + ".yaml", resolver=StratusResolver( handler ) )

    def run(self):
        port = self.getParameter( 'PORT', 5000 )
        self.db.create_all( )
        return self.app.run( int( port ) )

    def getParameter(self, name: str, default = None ) -> str:
        parm = self.parms.get( name, default )
        if parm is None: raise Exception( "Missing required stratus parameter in settings.ini: " + name )
        return parm

    def render_server_error( ex: Exception ):
        print( str( ex ) )
        traceback.print_exc()
        return Response(response=json.dumps({ 'message': getattr(ex, 'message', repr(ex)), "code": 500 } ), status=500, mimetype="application/json")

    def make_celery( self, app: Flask ):
        celery = Celery( app.import_name, backend=app.config['DATABASE_URI'], broker=app.config['CELERY_BROKER_URL'] )
        celery.conf.update(app.config)

        class ContextTask(celery.Task):
            def __call__(self, *args, **kwargs):
                with app.app_context():
                    return self.run(*args, **kwargs)

        celery.Task = ContextTask
        return celery

app = StratusApp()

if __name__ == "__main__":
    app.run()