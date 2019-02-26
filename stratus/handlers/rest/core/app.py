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
            if request.method == 'POST':    requestSpec: str = request.form["request"]
            else:                           requestSpec: str = request.get("request",None)
            client = self.core.getClient( "edas")
            requestDict: Dict = self.jsonRequest( requestSpec )
            task = client.request(  "exe", request=requestDict )
            return self.jsonResponse( dict( status="executing", id=task.id ) )


        @bp.route('/capabilities', methods=('GET'))
        def exe():
            requestSpec: str = request.get("request", None)

            client = self.core.getClient("edas")
            requestDict: Dict = self.jsonRequest(requestSpec)
            task = client.request("exe", request=requestDict)
            return self.jsonResponse(dict(status="executing", id=task.id))

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

        return bp




