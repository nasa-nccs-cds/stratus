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
            if request.method == 'POST':    requestDict: Dict = request.json
            else:                           requestDict: Dict = self.jsonRequest( request.args.get("request",None) )
            self.logger.info(f"Processing Request: '{str(requestDict)}'")
            current_tasks = self.core.processWorkflow( requestDict )
            self.logger.info("Current tasks: {} ".format(str(list(current_tasks.keys()))))
            for task in current_tasks.values(): self.addTask( task )
            return self.jsonResponse( dict( status="executing", id=requestDict['sid'] ), code=202 )

        # @bp.route('/result', methods=('GET'))
        # def exe():
        #     sid = request.args.get("sid", None)
        #     client = self.core.getClient()
        #     self.logger.info( f"{request.method}--> {str(requestDict)}" )
        #     task = client.request(  "exe", request=requestDict )
        #     tid = self.addTask( task )
        #     return self.jsonResponse( dict( status="executing", id=tid ), code=202 )

        @bp.route('/status', methods=('GET',))
        def status():
            cid = request.args.get("cid", None)
            statusMap = self.getStatus( cid )
            return self.jsonResponse( statusMap )

        @bp.route('/epas', methods=('GET',))
        def epas():
            epaList: List[str] = self.core.handlers.getEpas()
            return self.jsonResponse( dict(epas=epaList) )

        @bp.route('/capabilities', methods=('GET',))
        def capabilities():
            requestSpec: str = request.get("request", None)
            client = self.core.getClient()
            requestDict: Dict = self.jsonRequest(requestSpec)
            task = client.request("exe", request=requestDict)
            return self.jsonResponse(dict(status="executing", id=task.id))

        return bp




