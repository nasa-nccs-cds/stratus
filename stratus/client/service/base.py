import abc
from stratus.request.operation import Operation
from stratus.client.api import Client

class ServiceClient(Client):
    __metaclass__ = abc.ABCMeta

