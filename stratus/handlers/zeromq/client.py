from stratus.app.client import StratusClient, stratusrequest
import zmq, zmq.auth, traceback, json
from stratus_endpoint.util.config import StratusLogger, UID
from threading import Thread
from typing import Dict, Optional, List
from stratus.util.parsing import s2b, b2s
from stratus_endpoint.handler.base import TaskHandle, Status, TaskResult, FailedTask
from zmq.auth.thread import ThreadAuthenticator
import os, pickle, queue
import xarray as xa
from enum import Enum
MB = 1024 * 1024

class ConnectionMode():
    BIND = 1
    CONNECT = 2
    DefaultPort = 4336

    def __init__(self, **kwargs):
        self.logger = StratusLogger.getLogger()
        self.cert_dir = os.path.expanduser( kwargs.get("certificate_path", "~/.stratus/zmq" ) )
        self.public_keys_dir = os.path.join( self.cert_dir, 'public_keys' )
        self.secret_keys_dir = os.path.join( self.cert_dir, 'private_keys' )
        if not ( os.path.exists(self.public_keys_dir) and os.path.exists(self.secret_keys_dir) ):
            raise Exception( f"Must copy the contents of the zmq server certificates directory to {self.cert_dir}")

    def connectSocket( self, socket: zmq.Socket, host: str, port: int ):
        self.addClientAuth( socket )
        socket.connect("tcp://{0}:{1}".format( host, port ) )
        return port

    def addClientAuth( self, socket: zmq.Socket ):
        self.logger.info( f"Adding ZMQ client auth using keys from dir: {self.cert_dir}")
        client_secret_file = os.path.join( self.secret_keys_dir, "client.key_secret")
        client_public, client_secret = zmq.auth.load_certificate(client_secret_file)
        socket.curve_secretkey = client_secret
        socket.curve_publickey = client_public

        server_public_file = os.path.join( self.public_keys_dir, "server.key")
        server_public, _ = zmq.auth.load_certificate(server_public_file)
        socket.curve_serverkey = server_public

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
        self.context = None

    def init(self, **kwargs):
        try:
            if self.context is None:
                self.context = zmq.Context()
                self.connector = ConnectionMode( **self.parms )
                self.request_socket = self.context.socket(zmq.REQ)
                self.request_port = self.connector.connectSocket(self.request_socket, self.host_address, self.default_request_port )
                self.log("[1]Connected request socket to server {0} on port: {1}".format( self.host_address, self.request_port ) )
                local_stack = str( [ str(sl) + "\n" for sl in traceback.format_stack() ] )
                print( f"Initialized zmq client at:\n{local_stack}" )
                super(ZMQClient, self).init()

        except Exception as err:
            err_msg =  "\n-------------------------------\nWorker Init error: {0}\n{1}-------------------------------\n".format(err, traceback.format_exc() )
            self.logger.error(err_msg)
            self.logger.error(traceback.format_exc())
            self.shutdown()

    @stratusrequest
    def request(self, requestSpec: Dict, inputs: List[TaskResult] = None, **kwargs ) -> TaskHandle:
        response = self.sendMessage( "exe", requestSpec, **kwargs )
        self.log( f"Got exe response: {response}" )
        if "error" in response: raise Exception( f"Server Error: {response['error']}" )
        status = Status.decode( response.get('status') )
        self.log( str(response) )
        response_manager = ResponseManager( self.context, self.connector, response["rid"], self.host_address, self.response_port, status, self.cache_dir, **kwargs )
        response_manager.start()
        return zmqTask( self.cid, response_manager )

    def capabilities(self, ctype: str, **kwargs ) -> Dict:
        return self.sendMessage( "capabilities", {"type":ctype}, **kwargs )

    def log(self, msg: str ):
        self.logger.info( "[ZP] " + msg )

    def __del__(self):
        self.shutdown()

    def shutdown(self):
        if self.active:
            self.active = False
            try: self.request_socket.close()
            except Exception: pass

    def sendMessage(self, type: str, requestData: Dict, **kwargs ) -> Dict:
        requestId = requestData.get( "rid", UID.randomId(6) )
        msg = json.dumps( requestData )
        self.log( "Sending authenticated {} request {} on port {}, requestId = {}.".format( type, msg, str(self.request_port), requestId )  )
        try:
            message = "!".join( [ requestId, type, msg ] )
            self.request_socket.send_string( message )
            response = self.request_socket.recv_string()
        except zmq.error.ZMQError as err:
            self.logger.error( "Error sending message {0} on request socket: {1}".format( msg, str(err) ) )
            response = str(err)
        parts = response.split("!")
        response = json.loads(parts[1])
        response["rid"] = requestId
        return response

class ResponseManager(Thread):

    def __init__(self, context: zmq.Context, connector: ConnectionMode, rid: str, host: str, port: int, status: Status, cache_dir: str, **kwargs ):
        Thread.__init__(self)
        self.context = context
        self._connector = connector
        self.logger = StratusLogger.getLogger()
        self.host = host
        self.port = port
        self.requestId = rid
        self.active = True
        self.mstate = MessageState.RESULT
        self.setName('STRATUS zeromq client Response Thread')
        self.cached_results: queue.Queue[TaskResult] = queue.Queue()
        self.setDaemon(True)
        self.cacheDir = os.path.expanduser( cache_dir )
        self.log("Created RM, cache dir = " + self.cacheDir )
        self._status = status
        self._exception = None

    def cacheResult(self, header: Dict, data: Optional[xa.Dataset] ):
        self.logger.info( "Caching result: " + str(header) )
        dataList = [] if data is None else [data]
        self.cached_results.put( TaskResult( header, dataList )  )

    def getResult( self, **kwargs ) ->  Optional[TaskResult]:
        timeout = kwargs.get("timeout")
        block = kwargs.get("block")
        try:                 return self.cached_results.get( block, timeout )
        except queue.Empty:  return None

    def run(self):
        response_socket = None
        try:
            self.log("Run RM thread")
            response_socket: zmq.Socket = self.context.socket( zmq.SUB )
            response_port = self._connector.connectSocket( response_socket, self.host, self.port )
            response_socket.subscribe( s2b( self.requestId ) )
            self.log("Connected response socket on port {} with subscription (client/request) id: '{}', active = {}".format( response_port, self.requestId, str(self.active) ) )
            while( self.active ):
                self.processNextResponse( response_socket )

        except Exception as err:
            self.log( "ResponseManager error: " + str(err) )
            self.cacheResult( {  "status":"error", "error": str(err) }, None )
        finally:
            if response_socket: response_socket.close()

    def term(self):
        if self.active:
            self.active = False

    def log(self, msg: str ):
        self.logger.info( "[RM] " + msg )

    def exception(self) -> Optional[Exception]:
        return self._exception

    def processNextResponse(self, socket: zmq.Socket ):
        try:
            self.log("Awaiting responses" )
            response = socket.recv_multipart()
            sId = b2s( response[0] )
            header = json.loads( b2s( response[1] ) )
            type = header["type"]
            self._status =  Status.decode( header["status"] )

            if type == "xarray" and len(response) > 2:
                dataset = pickle.loads(response[2])
                self.cacheResult( header, dataset )
            elif self._status == Status.ERROR:
                self._exception = Exception( header["error"] )
            else:
                self.cacheResult( header, None )

            self.log(f"[{sId}]: Received response: " +  str( header ) + ", new status = " + str( self._status )  + ", exception = " + str( self._exception )  )

        except Exception as err:
            self.log( "EDAS error: {0}\n{1}\n".format(err, traceback.format_exc() ) )
            self._exception = err

    def getStatus(self):
        return self._status


class zmqTask(TaskHandle):

    def __init__(self, cid: str, manager: ResponseManager, **kwargs):
        super(zmqTask,self).__init__( rid=manager.requestId, cid=cid, **kwargs )
        self.logger = StratusLogger.getLogger()
        self.manager = manager

    def getResult( self, **kwargs ) ->  Optional[TaskResult]:
        return self.manager.getResult(**kwargs)

    def exception(self) -> Optional[Exception]:
        return self.manager.exception()

    def status(self) ->  Status:
        return self.manager.getStatus()

    def __del__(self):
        self.manager.term()


