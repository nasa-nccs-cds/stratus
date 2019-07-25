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
        vid = inputSpec['name']
        variable: xa.DataArray = dset.data_vars[ vid ]
        result_arrays = self.operate( vid, variable )
        resultDataset = xa.Dataset( result_arrays, dset.coords, dset.attrs)
        return TaskResult( kwargs, [ resultDataset ] )

    def operate(self, vid: str, variable: xa.DataArray )-> Dict[str,xa.DataArray] :
        opSpecs = self.request['operation']
        result_arrays: Dict[str,xa.DataArray] = {}
        for opSpec in opSpecs:
            opId = opSpec['name'].split(':')[1]
            opAxis = opSpec['axis']
            new_vid = "-".join([vid, opAxis, opId])
            if   opId == "mean": result_arrays[new_vid] = variable.mean( dim=opAxis )
            elif opId == "ave":  result_arrays[new_vid] = variable.mean( dim=opAxis)
            elif opId == "max":  result_arrays[new_vid] = variable.max( dim=opAxis )
            elif opId == "min":  result_arrays[new_vid] = variable.min( dim=opAxis )
            elif opId == "sum":  result_arrays[new_vid] = variable.sum( dim=opAxis )
            elif opId == "std":  result_arrays[new_vid] = variable.std( dim=opAxis )
            else: raise Exception( f"Unknown operation: '{opId}'")
        return result_arrays