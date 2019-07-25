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
        result_datasets: List[xa.Dataset] = []
        for input in self.inputs:
            dset : xa.Dataset = input.getDataset()
            result_datasets.append( dset.mean( dim='time') )
        return TaskResult( kwargs, result_datasets )


    def execute1(self, **kwargs) -> TaskResult:
        print( f"Executing request {self.request}" )
        result_datasets: List[xa.Dataset] = []
        for input in self.inputs:
            result_vars = {}
            dset : xa.Dataset = input.getDataset()
            for id, var in dset.data_vars.items():
                result_vars[id] = self.op( var )
            result_datasets.append( xa.Dataset( result_vars, dset.coords, dset.attrs ) )
        return TaskResult( kwargs, result_datasets )

    def op(self, var: xa.DataArray ) -> xa.DataArray:
        operation = self.request['operation']
        return var.mean( dim='time' )