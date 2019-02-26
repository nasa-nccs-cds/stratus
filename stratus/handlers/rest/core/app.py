from stratus.handlers.rest.app import StratusApp
from flask import Flask, Response, request, Blueprint
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Optional
from stratus.handlers.rest.app import RestAPIBase
import os, abc, json

class RestAPI(RestAPIBase):

    def _createBlueprint( self, app: Flask ) -> Blueprint:
        bp = Blueprint( self.name, __name__, url_prefix=f'/{self.name}' )

        @bp.route('/exe', methods=('GET', 'POST'))
        def exe():
            if request.method == 'GET':
                requestArgs = { key.lower():value for key,value in request.args.items() }
                requestSpec: str = requestArgs.get("request",None)
                client = self.core.getClient( "edas")
                requestDict: Dict = self.jsonRequest( requestSpec )
                if requestDict["status"] == "error": return self.jsonResponse( requestDict, 400 )
                task = client.request(  "exe", request=requestDict )
                return self.jsonResponse( dict( status="executing", id=task.id ) )


    # response = server.request( "stat", id=response['id'] )
    # print( response )
    #
    # response = server.request( "kill", id=response['id'] )
    # print( response ) )

#                if rType.lower() == "execute":
#                    rInputs: str = requestArgs.get("datainputs", None)
#             if request.method == 'POST':
#                 requestArgs = {key.lower(): value for key, value in request.form.items()}
#                 rType: str = requestArgs.get("request", None)
#                 assert rType is not None, "Missing 'Request' argument"
#                if rType.lower() == "execute":
#                    rInputs: str = requestArgs.get("datainputs", None)
                return "Hello!"
        return bp




