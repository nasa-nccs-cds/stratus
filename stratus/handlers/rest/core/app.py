from stratus.handlers.rest.app import StratusApp
from flask import Flask, Response, request, Blueprint
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Optional
from stratus.handlers.rest.app import RestAPIBase
import os, abc

class RestAPI(RestAPIBase):

    def _createBlueprint( self ) -> Blueprint:
        bp = Blueprint( self.name, __name__, url_prefix=f'/{self.name}' )

        @bp.route('/exe', methods=('GET', 'POST'))
        def wps():
            if request.method == 'GET':
                requestArgs = { key.lower():value for key,value in request.args.items() }
                variable: str = requestArgs.get("variable",None)
                domain: str = requestArgs.get("domain", None)
                operation: str = requestArgs.get("operation", None)
                clients = self.core.getClient( "edas")
                assert len(clients), "Can't find edas client"
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




