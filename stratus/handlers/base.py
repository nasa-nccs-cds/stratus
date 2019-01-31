import string, random, abc, os, yaml, json
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Optional
from stratus.handlers.client import StratusClient, ClientFactory

class Handler:

    def __init__(self, **kwargs):
        self.parms = kwargs
        self.name = self['name']
        self.type = self['type']
        self._client = None

    def __getitem__( self, key: str ) -> str:
        result =  self.parms.get( key, None )
        assert result is not None, "Missing required parameter in {}: {} ".format( self.__class__.__name__, key )
        return result

    def parm(self, key: str, default: str ) -> str:
        return self.parms.get( key, default  )

    @property
    def client(self) -> StratusClient:
        if self._client is None:
            self._client = ClientFactory.getClient(**self.parms)
        return self._client

    def __repr__(self):
        return json.dumps( self.parms )

class Handlers:
    HERE = os.path.dirname(__file__)
    SPEC_FILE = os.path.join( HERE, 'handlers.yaml')

    def __init__(self):
        self._handlers: Dict[str, Handler] = {}
        spec = self.load_spec()
        for service_spec in spec['services']:
            service = Handler(**service_spec)
            self._handlers[ service.name] = service

    def load_spec(self):
        with open( self.SPEC_FILE, 'r') as stream:
            data_loaded = yaml.load(stream)
        return data_loaded

    def __getitem__( self, key: str ) -> Handler:
        result =  self._handlers.get(key, None)
        assert result is not None, "Attempt to access unknown handler in Handlers: {} ".format( key )
        return result

    def getClients( self, epa, **kwargs ) -> List[StratusClient]:
        return [service.client for service in self._handlers.values() if service.client.handles( epa, **kwargs)]

    @property
    def available(self) -> Dict[str, Handler]:
        return self._handlers

    def __repr__(self):
        return json.dumps({key: s.parms for key,s in self._handlers.items()})

handlers = Handlers()

# class Handler:
#     __metaclass__ = abc.ABCMeta
#
#     @classmethod
#     def randomStr(cls, length) -> str:
#         tokens = string.ascii_uppercase + string.ascii_lowercase + string.digits
#         return ''.join(random.SystemRandom().choice(tokens) for _ in range(length))
#
#     @abc.abstractmethod
#     def handles(self, op: str )-> bool : pass
#
#     @abc.abstractmethod
#     def processRequest(self, op: str, **kwargs ): pass
#
#
# class DebugHandler(Handler):
#
#     def handles(self, op: str )-> bool : return True
#
#     def processRequest(self, op: str, **kwargs ):
#         rid = kwargs.get("id", self.randomStr(8) )
#         kwargs.update( { "op" : op, "id": rid, "status": "complete" } )
#         return kwargs
#
# class Handlers:
#     handlers = [ DebugHandler() ]
#
#     @classmethod
#     def processRequest(cls, op, **kwargs ):
#         handler = cls.getHandler( op )
#         return handler.processRequest( op, **kwargs )
#
#     @classmethod
#     def getHandler( cls, op: str ):
#         for handler in cls.handlers:
#             if handler.handles( op ):
#                 return handler
#         return None
#
#     @classmethod
#     def addHandler(cls, handler ):
#         cls.handlers.insert( 0, handler )

if __name__ == "__main__":
    mgr = Handlers()
    print( str(mgr) )