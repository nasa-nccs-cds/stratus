import json, string, random, abc, os
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
from stratus.handlers.base import Handler
from stratus.handlers.client import StratusClient
from stratus.util.config import Config, StratusLogger
from .client import ZMQClient
import zmq, traceback, time, logging, xml, socket
from typing import List, Dict, Sequence, Set
import random, string, os, queue, datetime
from stratus.util.parsing import s2b, b2s
from enum import Enum
MB = 1024 * 1024

class ServiceHandler( Handler ):

    def __init__(self, **kwargs ):
        htype = os.path.basename(os.path.dirname(__file__))
        super(ServiceHandler, self).__init__( htype, **kwargs )

    def newClient(self) -> StratusClient:
        return ZMQClient( **self.parms )

# class ExecutionCallback:
#   def success( results: xml.Node  ): pass
#   def failure( msg: str ): pass

class Response:

    def __init__(self, _rtype: str, _clientId: str, _responseId: str ):
        self.clientId = _clientId
        self.responseId = _responseId
        self.rtype = _rtype
        self._body = None

    def id(self) -> str:
        return self.clientId + ":" + self.responseId

    def message(self) -> str: return self._body.strip()

    def __str__(self) -> str: return self.__class__.__name__ + "[" + self.id() + "]: " + str(self._body)

class Message ( Response ):

    def __init__(self,  clientId: str,  responseId: str,  message: Dict ):
        super(Message, self).__init__( "message", clientId, responseId )
        self._body = json.dumps( message )

class ErrorReport(Response):

    def __init__( self,  clientId: str,  responseId: str,  message: Dict ):
        super(ErrorReport, self).__init__( "error", clientId, responseId )
        self._body = json.dumps( message )


class DataPacket(Response):

    def __init__( self,  clientId: str,  responseId: str,  header: str, data: bytes = bytearray(0)  ):
        super(DataPacket, self).__init__( "data", clientId, responseId )
        self._body =  header
        self._data = data

    def hasData(self) -> bool:
        return ( self._data is not None ) and ( len( self._data ) > 0 )

    def getTransferHeader(self) -> bytes:
        return s2b( self._body )

    def getHeaderString(self) -> str:
        return self._body

    def getTransferData(self) -> bytes:
        return self._data

    def getRawData(self) -> bytes:
        return self._data

    def toString(self) -> str: return \
        "DataPacket[" + self._body + "]"


class Responder:

    def __init__( self,  _context: zmq.Context,  _client_address: str,  _response_port: int ):
        super(Responder, self).__init__()
        self.logger =  StratusLogger.getLogger()
        self.context: zmq.Context =  _context
        self.response_port = _response_port
        self.executing_jobs: Dict[str,Response] = {}
        self.status_reports: Dict[str,str] = {}
        self.clients: Set[str] = set()
        self.client_address = _client_address
        self.socket: zmq.Socket = self.initSocket()

    def registerClient( self, client: str ):
        self.clients.add(client)

    def sendResponse( self, msg: Response ):
        self.logger.info( "@@R: Post Message to response queue: " + str(msg) )
        self.doSendResponse(msg)

    def sendDataPacket( self, data: DataPacket ):
        self.logger.info( "@@R: Posting DataPacket to response queue: " + str(data) )
        self.doSendResponse( data )
        self.logger.info("@@R: POST COMPLETE ")

    def doSendResponse( self,  r: Response ):
        if( r.rtype == "message" ):
            packaged_msg: str = self.doSendMessage( r )
            dateTime =  datetime.datetime.now()
            self.logger.info( "@@R: Sent response: " + r.id() + " (" + dateTime.strftime("MM/dd HH:mm:ss") + "), content sample: " + packaged_msg.substring( 0, min( 300, len(packaged_msg) ) ) );
        elif( r.rtype == "data" ):
            self.doSendDataPacket( r )
        elif( r.rtype == "error" ):
                self.doSendErrorReport( r )
        else:
            self.logger.error( "@@R: Error, unrecognized response type: " + r.rtype )
            self.doSendErrorReport( ErrorReport( r.clientId, r.responseId, "Error, unrecognized response type: " + r.rtype ) )

    def doSendMessage(self, msg: Response, type: str = "response") -> str:
        msgStr = str(msg.message())
        self.logger.info("@@R: Sending {} MESSAGE: {}".format( type, msgStr ) )
        self.socket.send_multipart( [ s2b( msg.clientId ), s2b( msg.responseId ), s2b( type ), s2b( msgStr )  ] )
        return msgStr

    def doSendErrorReport( self, msg: Response  ):
        return self.doSendMessage( msg, "error")

    def doSendDataPacket( self, dataPacket: DataPacket ):
        multipart_msg = [ s2b( dataPacket.clientId ), s2b( dataPacket.responseId ), b"data", dataPacket.getTransferHeader() ]
        if dataPacket.hasData():
            bdata: bytes = dataPacket.getTransferData()
            multipart_msg.append( bdata )
            self.logger.info("@@R: Sent data packet for " + dataPacket.id() + ", data Size: " + str(len(bdata)) )
            self.logger.info("@@R: Data header: " + + dataPacket.getHeaderString())
        else:
            self.logger.info( "@@R: Sent data header only for " + dataPacket.id() + "---> NO DATA!" )

        self.socket.send_multipart( multipart_msg )

    def setExeStatus( self, cId: str, rid: str, status: str ):
        self.status_reports[rid] = status
        try:
            if status.startswith("executing"):
                self.executing_jobs[rid] = Response( "executing", cId, rid )
            elif (status.startswith("error")) or (status.startswith("completed") ):
                del self.executing_jobs[rid]
        except Exception: pass

    def initSocket(self) -> zmq.Socket:
        socket: zmq.Socket   = self.context.socket(zmq.PUB)
        try:
            socket.bind( "tcp://{}:{}".format( self.client_address, self.response_port ) )
            self.logger.info( "@@R: --> Bound response socket to client at {} on port: {}".format( self.client_address, self.response_port ) )
        except Exception as err:
            self.logger.error( "@@R: Error initializing response socket on {}, port {}: {}".format( self.client_address, self.response_port, err ) )
            self.logger.error(traceback.format_exc())
        return socket

    def close_connection( self ):
        try:
            for response in self.executing_jobs.values():
                self.doSendErrorReport( self.socket, ErrorReport(response.clientId, response.responseId, "Job terminated by server shutdown.") );
            self.socket.close()
        except Exception: pass

