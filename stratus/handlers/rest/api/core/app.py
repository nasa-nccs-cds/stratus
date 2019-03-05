from stratus.handlers.rest.app import StratusApp
from flask import Flask, Response, request, Blueprint, make_response
from stratus_endpoint.handler.base import Task, Status, TaskResult
import pickle
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Optional
from stratus.handlers.rest.app import RestAPIBase
import os, abc, json

class RestAPI(RestAPIBase):
    debug = False

    def _addRoutes(self, bp: Blueprint):

        @bp.route('/exe', methods=('GET', 'POST'))
        def exe():
            if request.method == 'POST':    requestDict: Dict = request.json
            else:                           requestDict: Dict = self.jsonRequest( request.args.get("request",None) )
            if self.debug: self.logger.info(f"Processing Request: '{str(requestDict)}'")
            current_tasks = self.core.processWorkflow( requestDict )
            if self.debug: self.logger.info("Current tasks: {} ".format(str(list(current_tasks.items()))))
            for task in current_tasks.values(): self.addTask( task )
            return self.jsonResponse( dict( status="executing", id=requestDict['sid'] ), code=202 )

        @bp.route('/status', methods=('GET',))
        def status():
            cid = self.getParameter( "cid", None, False)
            statusMap = self.getStatus(cid)
            if self.debug: self.logger.info( "Status Map: " + str(statusMap) )
            return self.jsonResponse( statusMap )

        @bp.route('/result', methods=('GET',))
        def result():
            sid = self.getParameter("sid")
            task: Task = self.tasks.get( sid, None )
            assert task is not None, f"Can't find task for sid {sid}, current tasks: {str(list(self.tasks.keys()))}"
            result: Optional[TaskResult] = task.getResult()
            if result is None:
                return self.jsonResponse( dict(status="executing", id=task.sid) )
            else:
                response = make_response( pickle.dumps( result ) )
                response.headers.set('Content-Type', 'application/octet-stream')
                self.removeTask( sid )
                return response

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




