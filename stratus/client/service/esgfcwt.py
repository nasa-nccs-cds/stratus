from stratus.request.operation import Operation
from .base import ServiceClient

class EsgfCwtClient(ServiceClient):

    def execute( self, operation: Operation, **kwargs ):
        domains = kwargs.get("domains")