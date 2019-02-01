import string, random, abc, os, yaml, json
from typing import List, Dict, Any, Sequence, Callable, BinaryIO, TextIO, ValuesView, Optional
from stratus.handlers.client import StratusClient
from stratus.util.config import Config, StratusLogger
import importlib

import abc, sys, pkgutil

class Handler:
    __metaclass__ = abc.ABCMeta

    def __init__(self, htype: str, **kwargs):
        self.parms = kwargs
        self.name = self['name']
        self.type: str = htype
        self._client = None
        htype1 = self.parms.pop("type")
        assert htype1 == htype, "Sanity check of Handler type failed: {} vs {}".format(htype1,htype)

    def __getitem__( self, key: str ) -> str:
        result =  self.parms.get( key, None )
        assert result is not None, "Missing required parameter in {}: {} ".format( self.__class__.__name__, key )
        return result

    def parm(self, key: str, default: str ) -> str:
        return self.parms.get( key, default  )

    @abc.abstractmethod
    def newClient(self) -> StratusClient: pass

    @property
    def client(self) -> StratusClient:
        if self._client is None:
            self._client = self.newClient()
        return self._client

    def __repr__(self):
        return json.dumps( self.parms )

class Handlers:
    HERE = os.path.dirname(__file__)
    SPEC_FILE = os.path.join( HERE, 'handlers.yaml')

    def __init__(self):
        self.logger = StratusLogger.getLogger()
        self._handlers: Dict[str, Handler] = {}
        self._constructors: Dict[str, Callable[[], Handler]] = {}
        self.addConstructors()
        spec = self.load_spec()
        for service_spec in spec['services']:
            try:
                service = self.getHandler(service_spec)
                self._handlers[ service.name ] = service
            except Exception as err:
                err_msg = "Error registering handler for service {}: {}".format( service_spec.get("name",""), str(err) )
                print( err_msg )
                self.logger.error( err_msg )

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

    def addConstructor( self, type: str, handler_constructor: Callable[[], Handler]  ):
        print( "Adding constructor for " + type )
        self._constructors[type] = handler_constructor

    def getHandler(self, service_spec: Dict[str,str] ) -> Handler:
        type = service_spec.get('type',None)
        name = service_spec.get('type', "")
        if type is None:
            raise Exception( "Missing required 'type' parameter in service spec'{}'".format(name) )
        constructor = self._constructors.get( type, None )
        assert constructor is not None, "No Handler registered of type '{}' for service spec '{}'".format( type, name )
        return constructor( **service_spec )

    def listPackages(self):
        package = __import__("stratus")
        packages = []
        for loader, module_name, is_pkg in pkgutil.walk_packages(package.__path__, package.__name__ + '.'):
            if is_pkg: packages.append(module_name)
        return packages

    def addConstructors(self):
        packageList = self.listPackages()
        for package_name in packageList:
            try:
                module = importlib.import_module(package_name + ".base")
                constructor = getattr(module, "ServiceHandler")
                type = package_name.split(".")[-1]
                self.addConstructor( type, constructor )
            except Exception as err:
                self.logger.warn( "Unable to register constructor for {}: {} ({})".format( package_name, str(err), err.__class__.__name__ ) )

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