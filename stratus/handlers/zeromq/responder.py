import json, string, random, abc, os, pickle, collections
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
from stratus_endpoint.util.config import StratusLogger
from threading import Thread
import zmq, traceback, time, logging, xml, socket
from stratus.app.operations import Status
from stratus_endpoint.handler.base import TaskHandle
from stratus.app.operations import Workflow
from typing import List, Dict, Sequence, Set
import random, string, os, queue, datetime
from stratus.util.parsing import s2b, b2s, ia2s, sa2s, m2s
import xarray as xa

class StratusResponse:

    def __init__(self, rid: str, body: Dict ):
        self._id = rid
        self._body = body

    @property
    def id(self): return self._id

    @property
    def message(self) -> str: return json.dumps(self._body)

    def __str__(self) -> str: return "[" + self.__class__.__name__  + "]: " + self.message

class DataPacket(StratusResponse):

    def __init__( self, rid: str, header: Dict, data: bytes = bytearray(0)  ):
        super(DataPacket, self).__init__( rid, header )
        self._data = data

    def hasData(self) -> bool:
        return ( self._data is not None ) and ( len( self._data ) > 0 )

    def getTransferHeader(self) -> bytes:
        return s2b( self.message )

    def getTransferData(self) -> bytes:
        return self._data

    def getRawData(self) -> bytes:
        return self._data

    def toString(self) -> str: return \
        "DataPacket[" + self.message + "]"

class StratusZMQResponder(Thread):

    def __init__( self,  _context: zmq.Context, _response_port: int, **kwargs ):
        super(StratusZMQResponder, self).__init__()
        self.logger =  StratusLogger.getLogger()
        self.context: zmq.Context =  _context
        self.response_port = _response_port
        self.executing_jobs: Dict[str, StratusResponse] = {}
        self.status_reports: Dict[str,str] = {}
        self.client_address = kwargs.get( "client_address", "*" )
        self.socket: zmq.Socket = self.initSocket()

    def getDataPackets(self, rid: str, status: Status, workflow: Workflow ) -> List[DataPacket]:
        from stratus_endpoint.handler.base import TaskResult
        if (status == Status.COMPLETED):
            taskHandles: Dict[str,TaskHandle] = workflow.getResults()
            dataPackets:  List[DataPacket] = []
            for tid, taskHandle in taskHandles.items():
                taskResult: TaskResult = taskHandle.getResult()
                if taskResult is None:
                    self.logger.warn( f" Enpty result for task {tid} in request {rid}")
                elif taskResult.getResultClass() == "METADATA":
                    self.logger.info(f"@@R: process Metadata Task, header = {taskResult.header}")
                    metadata = taskResult.header
                    metadata["type"] = taskResult.getResultType()
                    metadata["status"] = str(Status.COMPLETED)
                    dataPackets.append( self.createMessage( rid, metadata ) )
                else:
                    datasets = taskResult.data
                    self.logger.info(f"@@R: process Task, Num datasets= {len(datasets)}, header = {taskResult.header}")
                    for dataset in datasets: dataPackets.append( self.createDataPacket(  rid, dataset ) )
            return dataPackets
        elif (status == Status.ERROR):
            return [ self.createMessage( rid, {"error": str(taskHandle.exception()) }) for tid, taskHandle in workflow.getResults() ]
        else:
            raise Exception( f"Unexpected Status in getDataPackets: {Status.str(status)}")

    def processWorkflows(self, workflows: Dict[str, Workflow]) -> List[str]:
        completed_requests = []
        for rid, workflow in workflows.items():
            status = workflow.status()
            self.logger.info( f"@@R: process Workflow {rid}, status= {status} " )
            self.setExeStatus( rid, status )
            if status in [Status.COMPLETED, Status.ERROR, Status.CANCELED]:
                dataPackets = self.getDataPackets( rid, status, workflow )
                self.logger.info(f"@@R: Sending Completed Results, Num dataPackets= {len(dataPackets)}" )
                for dataPacket in dataPackets:
                    self.sendDataPacket( dataPacket )
                completed_requests.append(rid)
        return completed_requests

    def sendDataPacket( self, dataPacket: DataPacket ):
        multipart_msg = [ s2b( dataPacket.id ), dataPacket.getTransferHeader() ]
        if dataPacket.hasData():
            bdata: bytes = dataPacket.getTransferData()
            multipart_msg.append( bdata )
            self.logger.info("@@R: Sent data packet for " + dataPacket.id + ", data Size: " + str(len(bdata)) )
            self.logger.info("@@R: Data header: " + dataPacket.message)
        else:
            self.logger.info( "@@R: Sent data header only for " + dataPacket.id + "---> NO DATA!   BODY = " + dataPacket.message )
        self.socket.send_multipart( multipart_msg )

    def setExeStatus( self, rid: str, status: Status ):
        self.status_reports[rid] = status
#        self.logger.info(f"@@R: --> Set Execution Status[{rid}]: {str(status)}")
        try:
            if status == Status.EXECUTING:
                self.executing_jobs[rid] = StratusResponse(rid, {"status": "executing"})
            elif  status == Status.ERROR or status == Status.COMPLETED:
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

    def shutdown(self):
        self.close_connection()

    def close_connection( self ):
        try:
            for response in self.executing_jobs.values():
                self.sendErrorMessage( f"Job {response.id} terminated  by server shutdown.")
            self.socket.close()
        except Exception: pass

    def sendMessage(self, rid: str, message: Dict = None):
        dataPacket = self.createMessage( rid, message )
        self.sendDataPacket(dataPacket)

    def sendErrorMessage(self, rid: str, message: str = None):
        self.sendMessage(rid, { "error": message }  )

    def createDataPacket( self, rid: str, dataset: xa.Dataset, metadata: Dict = None ) -> DataPacket:
        data = pickle.dumps(dataset, protocol=-1)
        header = metadata if metadata else {}
        header["type"] = "xarray"
        header["status"] = str( Status.COMPLETED )
        return DataPacket( rid, header, data )

    def createMessage(self, rid: str, message: Dict = None ) -> DataPacket:
        if "type" not in message: message["type"] = "message"
        return DataPacket( rid, message )

    def __del__(self):
        self.shutdown()
