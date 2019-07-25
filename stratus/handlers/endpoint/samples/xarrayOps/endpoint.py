from stratus_endpoint.handler.base import TaskHandle, TaskResult
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Optional, Callable
from stratus_endpoint.util.config import StratusLogger
from stratus_endpoint.handler.execution import Executable, ExecEndpoint
import xarray as xa
import abc


class XaOpsEndpoint(ExecEndpoint):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def createExecutable( self, requestSpec: Dict, inputs: List[TaskResult] = None, **kwargs ) -> Executable:
        return XaOpsExecutable( requestSpec, inputs, **kwargs )

    def shutdown(self, **kwargs ):
        return

    @abc.abstractmethod
    def capabilities(self, type: str, **kwargs ) -> Dict:
        return dict( epas = [ "xop*"] )

    @abc.abstractmethod
    def init( self ):
        return


class XaOpsExecutable(Executable):

    def execute(self, **kwargs) -> TaskResult:
        print( f"Executing request {self.request}" )
        inputSpec = self.request['input']
        dset: xa.Dataset = xa.open_dataset( inputSpec['uri'] )
        variable: xa.Variable = dset.variables[ inputSpec['name'] ]
        result_arrays = self.operate( variable )
        resultDataset = xa.Dataset( result_arrays, dset.coords, dset.attrs)
        return TaskResult( kwargs, [ resultDataset ] )

    def operate(self, variable: xa.Variable )-> List[xa.DataArray] :
        opSpecs = self.request['operation']
        result_arrays: List[xa.DataArray] = []
        for opSpec in opSpecs:
            opId = opSpec['name'].split(':')[1]
            opAxis = opSpec['axis']
            if   opId == "ave": result_arrays.append( variable.ave( dim=opAxis ) )
            elif opId == "max": result_arrays.append( variable.max( dim=opAxis ) )
            elif opId == "min": result_arrays.append( variable.min( dim=opAxis ) )
            elif opId == "std": result_arrays.append( variable.std( dim=opAxis ) )
            else: raise Exception( f"Unknown operation: '{opId}'")
        return result_arrays