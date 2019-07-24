from stratus_endpoint.handler.base import TaskHandle, Endpoint, TaskResult
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Optional, Callable
from stratus_endpoint.util.config import StratusLogger
from stratus_endpoint.handler.execution import Executable, TaskExecHandler
import abc


class ExecEndpoint(Endpoint):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def createExecutable( self, rid: str, requestSpec: Dict, inputs: List[TaskResult] = None, **kwargs ) -> Executable:
        return XaOpsExecutable( rid, requestSpec, inputs, **kwargs )

    def shutdown(self, **kwargs ):
        return

    @abc.abstractmethod
    def capabilities(self, type: str, **kwargs ) -> Dict:
        return dict( epas = [ "xop*"] )

    @abc.abstractmethod
    def init( self ):
        return


class XaOpsExecutable(Executable):

    @abc.abstractmethod
    def execute(self, **kwargs) -> TaskResult:
        print( f"Executing request {self.request}" )
        return TaskResult( kwargs )