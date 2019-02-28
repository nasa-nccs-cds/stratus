import json, string, random, abc, os, pickle, collections
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
from stratus.handlers.base import Handler
from stratus.handlers.client import StratusClient
from stratus.util.config import Config, StratusLogger
from .client import RestClient
from threading import Thread
import zmq, traceback, time, logging, xml, socket
from stratus_endpoint.handler.base import Task, Status
from typing import List, Dict, Sequence, Set
import random, string, os, queue, datetime
from stratus.util.parsing import s2b, b2s, ia2s, sa2s, m2s
import xarray as xa
from enum import Enum
MB = 1024 * 1024

class ServiceHandler( Handler ):

    def __init__(self, **kwargs ):
        htype = os.path.basename(os.path.dirname(__file__))
        super(ServiceHandler, self).__init__( htype, **kwargs )

    def newClient(self) -> StratusClient:
        return RestClient( **self.parms )