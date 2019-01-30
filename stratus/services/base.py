import string, random, abc, os, yaml, json
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Optional

class Service:

    def __init__(self, **kwargs):
        self.parms = kwargs
        self.name = self['name']
        self.type = self['type']
        self.client = None

    def __getitem__( self, key: str ) -> str:
        result =  self.parms.get( key, None )
        assert result is not None, "Missing required parameter in {}: {} ".format( self.__class__.__name__, key )
        return result

    def parm(self, key: str, default: str ) -> str:
        return self.parms.get( key, default  )

    def client(self):
        if self.client is None:
            self.client = ClientManager.getClient( self.type, **self.parms )
        return self.client

    def __repr__(self):
        return json.dumps( self.parms )

class ServiceManager:
    HERE = os.path.dirname(__file__)
    SPEC_FILE = os.path.join( HERE, 'services.yml')

    def __init__(self):
        self._services: Dict[str,Service] = {}
        spec = self.load_spec()
        for service_spec in spec['services']:
            service = Service( **service_spec )
            self._services[ service.name] = service

    def load_spec(self):
        with open( self.SPEC_FILE, 'r') as stream:
            data_loaded = yaml.load(stream)
        return data_loaded

    def __getitem__( self, key: str ) -> Service:
        result =  self._services.get(key, None)
        assert result is not None, "Attempt to access unknown service in ServiceManager: {} ".format( key )
        return result

    @property
    def services(self) -> Dict[str,Service]:
        return self._services

    def __repr__(self):
        return json.dumps({key: s.parms for key,s in self._services.items()})

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
    mgr = ServiceManager()
    print( str(mgr) )