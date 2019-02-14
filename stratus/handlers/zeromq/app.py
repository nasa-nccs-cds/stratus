from stratus.handlers.app import StratusCore
import json, string, random, abc, os
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple
from stratus.util.config import Config, StratusLogger
import zmq, traceback, time, logging, xml, socket
from typing import List, Dict, Sequence, Set
import random, string, os, queue, datetime
from stratus.handlers.zeromq.base import Responder, ErrorReport, DataPacket, Message, Response
from stratus.handlers.manager import handlers
from enum import Enum
MB = 1024 * 1024

class StratusApp(StratusCore):

    def __init__( self, **kwargs ):
        StratusCore.__init__(self, **kwargs )
        self.logger =  StratusLogger.getLogger()
        self.active = True
        self.zeromq_parms = self.getConfigParms('zeromq')
        self.parms = self.getConfigParms('stratus')
        self.client_address = self.zeromq_parms["client.address"]
        self.request_port = self.zeromq_parms.get( "request_port", 4556 )
        self.response_port = self.zeromq_parms.get( "response_port", 4557 )

    def initSocket(self, client_address, request_port):
        try:
            self.request_socket.bind( "tcp://{}:{}".format( client_address, request_port ) )
            self.logger.info( "@@Portal --> Bound request socket to client at {} on port: {}".format( client_address, request_port ) )
        except Exception as err:
            self.logger.error( "@@Portal: Error initializing request socket on {}, port {}: {}".format( client_address,  request_port, err ) )
            self.logger.error( traceback.format_exc() )

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

        try:
            self.zmqContext: zmq.Context = zmq.Context()
            self.request_socket: zmq.Socket = self.zmqContext.socket(zmq.REP)
            self.responder = Responder( self.zmqContext, self.client_address, self.response_port )
            self.handlers = {}
            self.initSocket( self.client_address, self.request_port )

        except Exception as err:
            self.logger.error( "@@Portal:  ------------------------------- StratusApp Init error: {} ------------------------------- ".format( err ) )

        while self.active:
            self.logger.info(  "@@Portal:Listening for requests on port: {}, host: {}".format( self.request_port, self.getHostInfo() ) )
            request_header = self.request_socket.recv_string().strip().strip("'")
            parts = request_header.split("!")
            clientId = parts[0]
            rType = parts[1]
            self.responder.registerClient( clientId )
            try:
                timeStamp = datetime.datetime.now().strftime("MM/dd HH:mm:ss")
                self.logger.info( "@@Portal:  ###  Processing {} request @({})".format( rType, timeStamp) )
                if rType == "epas":
                    msg = { "epas": handlers.getEpas() }
                    self.sendResponseMessage( Message( clientId, "epas", json.dumps( msg ) ) )
                elif rType == "request":
                    if len(parts) <= 2: raise Exception( "Missing parameters to utility request")
                    request = json.loads( parts[2] )
                    responses = self.processWorkflow(request)
                    self.sendResponseMessage( Message( clientId, "response", json.dumps(responses) )  )
                elif rType == "quit" or rType == "shutdown":
                    self.sendResponseMessage( Message( clientId, "quit", "Terminating") )
                    self.logger.info("@@Portal: Received Shutdown Message")
                    exit(0)
                else:
                    msg = "@@Portal: Unknown request type: " + rType
                    self.logger.info(msg)
                    self.sendResponseMessage( Message(clientId, "error", msg) )
            except Exception as ex:
                # clientId = elem( self.taskSpec, 0 )
                # runargs = self.getRunArgs( self.taskSpec )
                # jobId = runargs.getOrElse("jobId", self.randomIds.nextString)
                self.logger.error( "@@Portal: Execution error: " + str(ex) )
                traceback.print_exc()
                self.sendResponseMessage( Message( clientId, "error", str(ex)) )

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

if __name__ == "__main__":
    from stratus.handlers.manager import Handlers
    app = StratusApp( settings=Handlers.getStratusFilePath( "stratus/handlers/zeromq/test_settings1.ini" ) )
    app.run()

