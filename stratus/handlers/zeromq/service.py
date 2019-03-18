import json, string, random, abc, os, pickle, collections
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
from stratus.handlers.base import Handler
from stratus.handlers.client import StratusClient
from stratus.util.config import StratusLogger
from .client import ZMQClient
from threading import Thread
import zmq, traceback, time, logging, xml, socket
from stratus_endpoint.handler.base import Task, Status
from stratus.handlers.core import StratusCore
from .app import StratusApp
from typing import List, Dict, Sequence, Set
import random, string, os, queue, datetime
from stratus.util.parsing import s2b, b2s, ia2s, sa2s, m2s
import xarray as xa
MB = 1024 * 1024

class ServiceHandler( Handler ):

    def __init__(self, **kwargs ):
        htype = os.path.basename(os.path.dirname(__file__))
        super(ServiceHandler, self).__init__( htype, **kwargs )

    def newClient( self, gateway=False ) -> StratusClient:
        return ZMQClient( gateway=gateway, **self.parms )

    def newApplication(self, core: StratusCore ) -> StratusApp:
        return StratusApp( core )


