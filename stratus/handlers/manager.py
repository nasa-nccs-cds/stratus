import string, random, abc, os, yaml, json
from typing import List, Dict, Any, Sequence, Callable, BinaryIO, TextIO, ValuesView, Optional
from stratus.handlers.client import StratusClient
from stratus.util.config import Config, StratusLogger
from .base import Handler
import importlib

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
        self.logger.info( "Adding constructor for " + type )
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
                msg = "Unable to register constructor for {}: {} ({})".format( package_name, str(err), err.__class__.__name__ )
                print( msg )
                self.logger.warn( msg )

handlers = Handlers()
