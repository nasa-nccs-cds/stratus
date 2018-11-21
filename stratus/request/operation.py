from .base import RequestObject
from .input import Input
from .domain import Domain

class Operation(Input):

    def __init__(self, name: str = None, **kwargs ):
        self._axes = kwargs.pop("axes", kwargs.pop("axis","") )
        super(Operation, self).__init__( name, **kwargs )

    @property
    def axes(self):
        return self._axes