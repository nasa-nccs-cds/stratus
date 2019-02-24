from stratus.handlers.client import StratusClient
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple
import importlib
import zmq, traceback, time, logging, xml, json
from stratus.util.config import Config, StratusLogger, UID
from threading import Thread
from typing import Sequence, List, Dict, Mapping, Optional
from stratus.util.parsing import s2b, b2s
from stratus_endpoint.handler.base import Task, Status, TaskResult
import random, string, os, pickle, queue
import xarray as xa
from enum import Enum
MB = 1024 * 1024

class ConnectionMode():
    BIND = 1
    CONNECT = 2
    DefaultPort = 4336

    @classmethod
    def bindSocket( cls, socket: zmq.Socket, server_address: str, port: int ):
        test_port = port if( port > 0 ) else cls.DefaultPort
        while( True ):
            try:
                socket.bind( "tcp://{0}:{1}".format(server_address,test_port) )
                return test_port
            except Exception as err:
                test_port = test_port + 1

    @classmethod
    def connectSocket( cls, socket: zmq.Socket, host: str, port: int ):
        socket.connect("tcp://{0}:{1}".format( host, port ) )
        return port

class MessageState(Enum):
    ARRAY = 0
    FILE = 1
    RESULT = 2

class ZMQClient(StratusClient):

    def __init__( self, **kwargs ):
        super(ZMQClient, self).__init__( "zeromq", **kwargs )
        self.host_address = self.parm( "host", "127.0.0.1" )
        self.default_request_port = int( self.parm( "request_port", 4556 ) )
        self.response_port = int( self.parm( "response_port", 4557 ) )


    def init(self, **kwargs):
        try:
            self.context = zmq.Context()
            self.request_socket = self.context.socket(zmq.REQ)
            self.request_port = ConnectionMode.connectSocket(self.request_socket, self.host_address, self.default_request_port )
            self.log("[1]Connected request socket to server {0} on port: {1}".format( self.host_address, self.request_port ) )
            super(ZMQClient, self).init()

        except Exception as err:
            err_msg =  "\n-------------------------------\nWorker Init error: {0}\n{1}-------------------------------\n".format(err, traceback.format_exc() )
            self.logger.error(err_msg)
            self.shutdown()

    def request(self, type: str, **kwargs ) -> Task:
        response = self.sendMessage( type, kwargs )
        self.log( str(response) )
        response_manager = ResponseManager( self.context, response["id"], self.host_address, self.response_port,   **kwargs )
        response_manager.start()
        return zmqTask( response_manager )

    def capabilities(self, type: str, **kwargs ) -> Dict:
        return self.sendMessage( type, kwargs )

    def log(self, msg: str ):
        self.logger.info( "[P] " + msg )

    def __del__(self):
        self.log(  " Portal client being deleted " )
        self.shutdown()

    def createResponseManager(self) -> "ResponseManager":
        return self.response_manager

    def shutdown(self):
        if self.active:
            self.log(  " ############################## Disconnect Portal Client from Server & shutdown Client ##############################"  )
            self.active = False
            try: self.request_socket.close()
            except Exception: pass
            if not (self.response_manager is None):
                self.response_manager.term()
                self.response_manager = None

    def sendMessage(self, type: str, requestData: Dict ) -> Dict:
        rid = requestData.get( "id", UID.randomId(6) )
        submissionId = self.clientID + rid
        msg = json.dumps( requestData )
        self.log( "Sending {} request {} on port {}, submissionId = {}.".format( type, msg, str(self.request_port), submissionId )  )
        try:
            message = "!".join( [ submissionId, type, msg ] )
            self.request_socket.send_string( message )
            response = self.request_socket.recv_string()
        except zmq.error.ZMQError as err:
            self.logger.error( "Error sending message {0} on request socket: {1}".format( msg, str(err) ) )
            response = str(err)
        parts = response.split("!")
        response = json.loads(parts[1])
        response["id"] = submissionId
        return response

    def waitUntilDone(self):
        self.response_manager.join()

class ResponseManager(Thread):

    def __init__(self, context: zmq.Context, subscribeId: str, host: str, port: int, **kwargs ):
        Thread.__init__(self)
        self.context = context
        self.logger = StratusLogger.getLogger()
        self.host = host
        self.port = port
        self.subscribeId = subscribeId
        self.active = True
        self.mstate = MessageState.RESULT
        self.setName('STRATUS zeromq client Response Thread')
        self.cached_results: queue.Queue[TaskResult] = queue.Queue()
        self.setDaemon(True)
        self.cacheDir = kwargs.get( "cacheDir",  os.path.expanduser( "~/.edas/cache") )
        self.log("Created RM, cache dir = " + self.cacheDir )

    def cacheResult(self, header: Dict, data: Optional[xa.Dataset] ):
        self.cached_results.put( TaskResult(header,data)  )

    def getResult( self, block=True, timeout=None ) -> Optional[TaskResult]:
        try:                 return self.cached_results.get( block, timeout )
        except queue.Empty:  return None

    def run(self):
        response_socket = None
        try:
            self.log("Run RM thread")
            response_socket: zmq.Socket = self.context.socket( zmq.SUB )
            response_port = ConnectionMode.connectSocket( response_socket, self.host, self.port )
            response_socket.subscribe( s2b( self.subscribeId ) )
            self.log("Connected response socket on port {} with subscription (client/request) id: '{}', active = {}".format( response_port, self.subscribeId, str(self.active) ) )
            while( self.active ):
                self.processNextResponse( response_socket )

        except Exception as err:
            self.log( "ResponseManager error: " + str(err) )
            self.cacheResult( { "error": str(err) }, None )
        finally:
            if response_socket: response_socket.close()

    def term(self):
        self.log("Terminate RM thread")
        if self.active:
            self.active = False

    def log(self, msg: str, maxPrintLen = 300 ):
        self.logger.info( "[RM] " + msg )

    def processNextResponse(self, socket: zmq.Socket ):
        try:
            self.log("Awaiting responses" )
            response = socket.recv_multipart()
            sId = b2s( response[0] )
            header = json.loads( b2s( response[1] ) )
            type = header["type"]
            self.log(f"[{sId}]: Received response: " +  str( header ) + ", size = " + str( len(response) ) )
            if type == "xarray" and len(response) > 2:
                dataset = pickle.loads(response[2])
                self.cacheResult( header, dataset )
            else:
                self.cacheResult( header, None )

        except Exception as err:
            self.log( "EDAS error: {0}\n{1}\n".format(err, traceback.format_exc() ), 1000 )


class zmqTask(Task):

    def __init__(self, manager: ResponseManager, **kwargs):
        super(zmqTask,self).__init__( manager.subscribeId, **kwargs )
        self.logger = StratusLogger.getLogger()
        self.manager = manager

    def getResult(self, block=True, timeout=None ) ->  Optional[TaskResult]:
        return self.manager.getResult(block,timeout)

    def status(self) ->  Status:
        return self._status


if __name__ == "__main__":
    client = ZMQClient()
    client.init( )
    response = client.request( "exe",  operations=[ dict( id="op1", epa="A" ), dict( id="op2", epa="B" ), dict( id="op3", epa="C" ), dict( id="op4", epa="D" ), dict( id="op5", epa="E" ), dict( id="op6", epa="X" ), dict( id="op7", epa="J" ), dict( id="op8", epa="C" ) ] )
    print ( "response = " + str( response ) )

