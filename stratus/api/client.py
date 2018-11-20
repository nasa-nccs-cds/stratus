import abc
from .request.operation import Operation

class Client:
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def execute( self, operation: Operation, **kwargs ):
        domains = kwargs.get("domains")
        async = kwargs.get("async",True)