from stratus.handlers.rest.app import StratusApp
from flask import Flask, Response, request, Blueprint
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Optional
from stratus.handlers.rest.app import RestAPIBase
import os, abc

class RestAPI(RestAPIBase):

    def _addRoutes(self, bp: Blueprint):

        @bp.route('/cwt', methods=('GET', 'POST'))
        def wps():
            if request.method == 'GET':
                requestArgs = { key.lower():value for key,value in request.args.items() }
                rType: str = requestArgs.get("request",None)
#                clients = self.getClients( "edas.")
#                assert len(clients), "Can't find edas client"
#                assert rType is not None, "Missing 'Request' argument"
#                if rType.lower() == "execute":
#                    rInputs: str = requestArgs.get("datainputs", None)
#             if request.method == 'POST':
#                 requestArgs = {key.lower(): value for key, value in request.form.items()}
#                 rType: str = requestArgs.get("request", None)
#                 assert rType is not None, "Missing 'Request' argument"
#                if rType.lower() == "execute":
#                    rInputs: str = requestArgs.get("datainputs", None)
                return "Hello!"





