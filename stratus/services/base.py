import string, random, abc, os, yaml, json
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Optional

class Service:

    def __init__(self, **kwargs):
        self.parms = kwargs
        self.name = self['name']
        self.api = self['api']
        self.type = self['type']

    def __getitem__( self, key: str ) -> str:
        result =  self.parms.get( key, None )
        assert result is not None, "Missing required parameter in {}: {} ".format( self.__class__.__name__, key )
        return result

    def parm(self, key: str, default: str ) -> str:
        return self.parms.get( key, default  )

    def __repr__(self):
        return json.dumps( self.parms )

class ServiceManager:
    HERE = os.path.dirname(__file__)
    SPEC_FILE = os.path.join( HERE, 'services.yml')

    def __init__(self):
        self.services = {}
        spec = self.load_spec()
        for service_spec in spec['services']:
            service = Service( **service_spec )
            self.services[ service.name ] = service

    def load_spec(self):
        with open( self.SPEC_FILE, 'r') as stream:
            data_loaded = yaml.load(stream)
        return data_loaded

    def __getitem__( self, key: str ) -> Service:
        result =  self.services.get( key, None )
        assert result is not None, "Attempt to access unknown service in ServiceManager: {} ".format( key )
        return result

    def findService( self, type: str, api: str = None ) -> Optional[Service]:
        for service in self.services.values():
            if service.type == type and ( (api is None) or (service.api == api) ): return service
        return None

    def __repr__(self):
        return json.dumps( { key: s.parms for key,s in self.services.items() } )

class Handler:
    __metaclass__ = abc.ABCMeta

    @classmethod
    def randomStr(cls, length) -> str:
        tokens = string.ascii_uppercase + string.ascii_lowercase + string.digits
        return ''.join(random.SystemRandom().choice(tokens) for _ in range(length))

    @abc.abstractmethod
    def handles(self, op: str )-> bool : pass

    @abc.abstractmethod
    def processRequest(self, op: str, **kwargs ): pass


class DebugHandler(Handler):

    def handles(self, op: str )-> bool : return True

    def processRequest(self, op: str, **kwargs ):
        rid = kwargs.get("id", self.randomStr(8) )
        kwargs.update( { "op" : op, "id": rid, "status": "complete" } )
        return kwargs

class Handlers:
    handlers = [ DebugHandler() ]

    @classmethod
    def processRequest(cls, op, **kwargs ):
        handler = cls.getHandler( op )
        return handler.processRequest( op, **kwargs )

    @classmethod
    def getHandler( cls, op: str ):
        for handler in cls.handlers:
            if handler.handles( op ):
                return handler
        return None

    @classmethod
    def addHandler(cls, handler ):
        cls.handlers.insert( 0, handler )

if __name__ == "__main__":
    mgr = ServiceManager()
    print( str(mgr) )