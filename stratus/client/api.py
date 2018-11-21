import abc
from stratus.request.operation import Operation

class Client:
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def execute( self, operation: Operation, **kwargs ):
        pass