from stratus.handlers.rest.app import StratusApp
from flask import Flask, Response, request
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Optional
import os
HERE = os.path.dirname(os.path.abspath(__file__))
SETTINGS_FILE = os.path.join( HERE, "wps_settings.ini" )

class WPSStratusApp(StratusApp):

    def __init__( self, **kwargs ):
        StratusApp.__init__( self, **kwargs )

    def create_app(self, parms: Dict):
        app = Flask("stratus", instance_relative_config=True)
        app.config.from_mapping(parms)
        app.register_error_handler(TypeError, self.render_server_error)

        @app.route('/wps', methods=('GET', 'POST'))
        def wps():
            if request.method == 'GET':
                requestArgs = { key.lower():value for key,value in request.args.items() }
                rType: str = requestArgs.get("request",None)
                assert rType is not None, "Missing 'Request' argument"
#                if rType.lower() == "execute":
#                    rInputs: str = requestArgs.get("datainputs", None)
            if request.method == 'POST':
                requestArgs = {key.lower(): value for key, value in request.form.items()}
                rType: str = requestArgs.get("request", None)
                assert rType is not None, "Missing 'Request' argument"
#                if rType.lower() == "execute":
#                    rInputs: str = requestArgs.get("datainputs", None)
                return "Hello!"

        return app




if __name__ == "__main__":
    app = WPSStratusApp( settings=SETTINGS_FILE )
    app.run()
