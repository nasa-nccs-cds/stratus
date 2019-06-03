from stratus.app.base import StratusServerApp
from stratus.app.core import StratusCore
import json
from stratus_endpoint.util.config import StratusLogger
import zmq, traceback
from typing import Dict
import queue, datetime
from .responder import StratusZMQResponder, StratusResponse
from stratus_endpoint.handler.base import Status, TaskFuture
from stratus_endpoint.handler.base import TaskHandle, Endpoint, TaskResult
MB = 1024 * 1024

class StratusApp(StratusServerApp):

    def __init__( self, core: StratusCore, **kwargs ):
        StratusServerApp.__init__(self, core, **kwargs)
        self.logger =  StratusLogger.getLogger()
        self.active = True
        self.parms = self.getConfigParms('stratus')
        self.client_address = self.parms.get( "client_address","*" )
        self.request_port = self.parms.get( "request_port", 4556 )
        self.response_port = self.parms.get( "response_port", 4557 )
        self.active_handlers = {}

    def initSocket( self ):
        try:
            self.request_socket.bind( "tcp://{}:{}".format( self.client_address, self.request_port ) )
            self.logger.info( "@@Portal --> Bound request socket to client at {} on port: {}".format( self.client_address, self.request_port ) )
        except Exception as err:
            self.logger.error( "@@Portal: Error initializing request socket on {}, port {}: {}".format( self.client_address,  self.request_port, err ) )
            self.logger.error( traceback.format_exc() )

    def addHandler(self, clientId, jobId, handler ):
        self.active_handlers[ clientId + "-" + jobId ] = handler
        return handler

    def removeHandler(self, clientId, jobId ):
        handlerId = clientId + "-" + jobId
        try:
            del self.active_handlers[ handlerId ]
        except:
            self.logger.error( "Error removing handler: " + handlerId + ", active handlers = " + str(list(self.active_handlers.keys())))

    def setExeStatus( self, submissionId: str, status: Status ):
        self.responder.setExeStatus( submissionId, status )

    def sendResponseMessage(self, msg: StratusResponse) -> str:
        request_args = [ msg.id, msg.message ]
        packaged_msg = "!".join( request_args )
        timeStamp =  datetime.datetime.now().strftime("MM/dd HH:mm:ss")
        self.logger.info( "@@Portal: Sending response {} on request_socket @({}): {}".format( msg.id, timeStamp, str(msg) ) )
        self.request_socket.send_string( packaged_msg )
        return packaged_msg

    def initInteractions(self):
        try:
            self.zmqContext: zmq.Context = zmq.Context()
            self.request_socket: zmq.Socket = self.zmqContext.socket(zmq.REP)
            self.responder = StratusZMQResponder(self.zmqContext, self.response_port, self.tasks, client_address = self.client_address)
            self.responder.start()
            self.initSocket()
            self.logger.info(  "@@Portal:Listening for requests on port: {}".format( self.request_port ) )

        except Exception as err:
            self.logger.error( "@@Portal:  ------------------------------- StratusApp Init error: {} ------------------------------- ".format( err ) )

    def updateInteractions(self):
        while self.request_socket.poll(0) != 0:
            request_header = self.request_socket.recv_string().strip().strip("'")
            parts = request_header.split("!")
            submissionId = str(parts[0])
            rType =  str(parts[1])
            request: Dict = json.loads(parts[2]) if len(parts) > 2 else ""
            try:
                self.logger.info( "@@Portal:  ###  Processing {} request: {}".format( rType, request) )
                if rType == "capabilities":
                    response = self.core.getCapabilities( request["type"] )
                    self.sendResponseMessage(StratusResponse(submissionId, response))
                elif rType == "exe":
                    if len(parts) <= 2: raise Exception( "Missing parameters to exe request")
                    request["rid"] = submissionId
                    self.logger.info( "Processing Request: '{}' '{}' '{}'".format( submissionId, rType, str(request)) )
                    self.submitWorkflow(request)                                                                                                             #   TODO: Send results when tasks complete.
                    response = { "status": "Executing" }
                    self.sendResponseMessage(StratusResponse(submissionId, response))
                elif rType == "quit" or rType == "shutdown":
                    response = {"status": "Terminating" }
                    self.sendResponseMessage(StratusResponse(submissionId, response))
                    self.logger.info("@@Portal: Received Shutdown Message")
                    exit(0)
                else:
                    msg = "@@Portal: Unknown request type: " + rType
                    self.logger.info(msg)
                    response = {"error": msg }
                    self.sendResponseMessage(StratusResponse(submissionId, response))
            except Exception as ex:
                tb = traceback.format_exc()
                self.logger.error( "@@Portal: Execution error: " + str(ex) )
                self.logger.error( tb )
                response = { "error": str(ex), "traceback": tb }
                self.sendResponseMessage(StratusResponse(submissionId, response))



    def term( self, msg ):
        self.logger.info( "@@Portal: !!EDAS Shutdown: " + msg )
        self.active = False
        self.logger.info( "@@Portal: QUIT PythonWorkerPortal")
        try: self.request_socket.close()
        except Exception: pass
        self.logger.info( "@@Portal: CLOSE request_socket")
        self.responder.close_connection()
        self.logger.info( "@@Portal: TERM responder")
        self.shutdown()
        self.logger.info( "@@Portal: shutdown complete")

if __name__ == "__main__":
    core = StratusCore( "test_settings1.ini" )
    app = core.getApplication()
    app.run()

