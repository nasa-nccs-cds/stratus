from stratus.handlers.app import StratusCore
import json, string, random, abc, os
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple
from stratus.util.config import Config, StratusLogger
import zmq, traceback, time, logging, xml, socket
from typing import List, Dict, Sequence, Set
import random, string, os, queue, datetime
from .base import Responder, ErrorReport, DataPacket, Message, Response
from enum import Enum
MB = 1024 * 1024

class StratusApp(StratusCore):

    def __init__( self ):
        StratusCore.__init__(self)
        self.logger =  StratusLogger.getLogger()
        self.active = True
        self.parms = self.getConfigParms('stratus')
        self.client_address = self.parms["client.address"]
        self.request_port = self.parms.get( "request_port", 4556 )
        self.response_port = self.parms.get( "response_port", 4557 )

        try:
            self.zmqContext: zmq.Context = zmq.Context()
            self.request_socket: zmq.Socket = self.zmqContext.socket(zmq.REP)
            self.responder = Responder( self.zmqContext, self.client_address, self.response_port )
            self.handlers = {}
            self.initSocket( self.client_address, self.request_port )


        except Exception as err:
            self.logger.error( "@@Portal:  ------------------------------- StratusApp Init error: {} ------------------------------- ".format( err ) )

    def initSocket(self, client_address, request_port):
        try:
            self.request_socket.bind( "tcp://{}:{}".format( client_address, request_port ) )
            self.logger.info( "@@Portal --> Bound request socket to client at {} on port: {}".format( client_address, request_port ) )
        except Exception as err:
            self.logger.error( "@@Portal: Error initializing request socket on port {}: {}".format( request_port, err ) )

    def sendErrorReport( self, clientId: str, responseId: str, msg: str ):
        self.logger.info("@@Portal-----> SendErrorReport[" + clientId +":" + responseId + "]" )
        self.responder.sendResponse( ErrorReport(clientId,responseId,msg) )

    def addHandler(self, clientId, jobId, handler ):
        self.handlers[ clientId + "-" + jobId ] = handler
        return handler

    def removeHandler(self, clientId, jobId ):
        handlerId = clientId + "-" + jobId
        try:
            del self.handlers[ handlerId ]
        except:
            self.logger.error( "Error removing handler: " + handlerId + ", existing handlers = " + str(self.handlers.keys()))

    def setExeStatus( self, clientId: str, rid: str, status: str ):
        self.responder.setExeStatus(clientId,rid,status)

    def sendArrayData( self, clientId: str, rid: str, origin: Sequence[int], shape: Sequence[int], data: bytes, metadata: Dict[str,str] ):
        self.logger.debug( "@@Portal: Sending response data to client for rid {}, nbytes={}".format( rid, len(data) ) )
        array_header_fields = [ "array", rid, self.ia2s(origin), self.ia2s(shape), self.m2s(metadata), "1" ]
        array_header = "|".join(array_header_fields)
        header_fields = [ rid, "array", array_header ]
        header = "!".join(header_fields)
        self.logger.debug("Sending header: " + header)
        self.responder.sendDataPacket( DataPacket( clientId, rid, header, data ) )


    def sendFile( self, clientId: str, jobId: str, name: str, filePath: str, sendData: bool ) -> str:
        self.logger.debug( "@@Portal: Sending file data to client for {}, filePath={}".format( name, filePath ) )
        with open(filePath, mode='rb') as file:
            file_header_fields = [ "array", jobId, name, os.path.basename(filePath) ]
            if not sendData: file_header_fields.append(filePath)
            file_header = "|".join( file_header_fields )
            header_fields = [ jobId,"file", file_header ]
            header = "!".join(header_fields)
            try:
                data =  bytes(file.read()) if sendData else None
                self.logger.debug("@@Portal ##sendDataPacket: clientId=" + clientId + " jobId=" + jobId + " name=" + name + " path=" + filePath )
                self.responder.sendDataPacket( DataPacket( clientId, jobId, header, data ) )
                self.logger.debug("@@Portal Done sending file data packet: " + header)
            except Exception as ex:
                self.logger.info( "@@Portal Error sending file : " + filePath + ": " + str(ex) )
                traceback.print_exc()
            return file.name


    def execUtility( self, utilSpec: Sequence[str] ) -> Message: pass
    def execute( self, taskSpec: Sequence[str] ) -> Response: pass
    def shutdown( self ): pass
    def getCapabilities( self, type: str ) -> Message: pass
    def describeProcess( self, utilSpec: Sequence[str] ) -> Message: pass
    def getVariableSpec( self, collId: str, varId: str ) -> Message: pass

    def sendResponseMessage( self, msg: Response ) -> str:
        request_args = [ msg.id(), msg.message() ]
        packaged_msg = "!".join( request_args )
        timeStamp =  datetime.datetime.now().strftime("MM/dd HH:mm:ss")
        self.logger.info( "@@Portal: Sending response {} on request_socket @({}): {}".format( msg.responseId, timeStamp, str(msg) ) )
        self.request_socket.send_string( packaged_msg )
        return packaged_msg


    # public static String getCurrentStackTrace() {
    #     try{ throw new Exception("Current"); } catch(Exception ex)  {
    #         Writer result = new StringWriter();
    #         PrintWriter printWriter = new PrintWriter(result);
    #         ex.printStackTrace(printWriter);
    #         return result.toString();
    #     }
    # }

    def getHostInfo(self) -> str:
        try:
            hostname = socket.gethostname()
            address = socket.gethostbyname(hostname)
            return  "{} ({})".format( hostname, address )
        except Exception as e:
            return "UNKNOWN"

    def run(self):
        while self.active:
            self.logger.info(  "@@Portal:Listening for requests on port: {}, host: {}".format( self.request_port, self.getHostInfo() ) )
            request_header = str( self.request_socket.recv(0) ).strip().strip("'")
            parts = request_header.split("!")
            self.responder.registerClient( parts[0] )
            try:
                timeStamp = datetime.datetime.now().strftime("MM/dd HH:mm:ss")
                self.logger.info( "@@Portal:  ###  Processing {} request: {} @({})".format( parts[1], request_header, timeStamp) )
                if parts[1] == "execute":
                    self.sendResponseMessage( self.execute(parts) )
                elif parts[1] == "util":
                    if len(parts) <= 2: raise Exception( "Missing parameters to utility request")
                    self.sendResponseMessage( self.execUtility(parts[2:]) )
                elif parts[1] == "quit" or parts[1] == "shutdown":
                    self.sendResponseMessage( Message(parts[0], "quit", "Terminating") )
                    self.logger.info("@@Portal: Received Shutdown Message")
                    exit(0)
                elif parts[1].lower() == "getcapabilities":
                    type = parts[2] if (len(parts) > 2) and len(parts[2].strip()) else "kernels"
                    self.sendResponseMessage( self.getCapabilities(type) )
                elif parts[1].lower() == "describeprocess":
                    self.sendResponseMessage( self.describeProcess(parts) )
                else:
                    msg = "@@Portal: Unknown request header type: " + parts[1]
                    self.logger.info(msg)
                    self.sendResponseMessage( Message(parts[0], "error", msg) )
            except Exception as ex:
                # clientId = elem( self.taskSpec, 0 )
                # runargs = self.getRunArgs( self.taskSpec )
                # jobId = runargs.getOrElse("jobId", self.randomIds.nextString)
                self.logger.error( "@@Portal: Execution error: " + str(ex) )
                traceback.print_exc()
                self.sendResponseMessage( Message( parts[0], "error", str(ex)) )

        self.logger.info( "@@Portal: EXIT EDASPortal")

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

    def ia2s( self, array: Sequence[int] ) -> str:
        return str(array).strip("[]")

    def sa2s( self, array: Sequence[str] ) -> str:
        return ",".join(array)

    def m2s( self, metadata: Dict[str,str] ) -> str:
        items = [ ":".join(item) for item in metadata.items() ]
        return ";".join(items)


