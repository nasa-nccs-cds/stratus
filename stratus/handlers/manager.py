import string, random, abc, os, yaml, json
from typing import List, Dict, Any, Sequence, Callable, BinaryIO, TextIO, ValuesView, Optional
from stratus.handlers.client import StratusClient
from stratus.util.config import Config, StratusLogger, UID
from stratus.handlers.base import Handler
import itertools, traceback
import importlib

class Handlers:
    HERE = os.path.dirname( __file__ )
    STRATUS_ROOT = os.path.dirname( os.path.dirname( HERE ) )

    def __init__(self, **kwargs ):
        self.logger = StratusLogger.getLogger()
        self._handlers: Dict[str, Handler] = {}
        self._parms = kwargs
        self._constructors: Dict[str, Callable[[], Handler]] = {}
        self.configSpec: Config = None

    def init(self, configSpec: Config, **kwargs ):
        if self.configSpec is None:
            self.configSpec = configSpec
            self.addConstructors()
            self._parms.update(kwargs)
            hspecs = self.getHandlerSpecs()
            for service_spec in hspecs:
                htype = service_spec["type"]
                try:
                    service = self.getHandler(service_spec)
                    self._handlers[ service.name ] = service
                    self.logger.info( f"Initialized stratus for service {htype}" )
                except Exception as err:
                    err_msg = "Error registering handler for service {}: {}".format( service_spec.get("name",""), str(err) )
                    print( err_msg )
                    self.logger.error( err_msg )

    def getHandlerSpecs(self):
        specs = []
        htypes = self.listAvailableHandlers()
        assert self.configSpec is not None, "Error, the handlers have not yet beeb initialized"
        for name, spec in self.configSpec.items():
            if spec.get("type") in htypes:
                spec["name"] = name
                specs.append( spec )
        return specs

    def getStratusFilePath(self, handlersFile: str) -> str:
        if handlersFile.startswith("/"): return handlersFile
        root = self._parms.get("home", self.STRATUS_ROOT )
        return os.path.join( root, handlersFile )

    def __getitem__( self, key: str ) -> Handler:
        result =  self._handlers.get(key, None)
        assert result is not None, "Attempt to access unknown handler in Handlers: {} ".format( key )
        return result

    def getClients( self, epa: str = None, **kwargs ) -> List[StratusClient]:
        assert self.configSpec is not None, "Error, the handlers have not yet been initialized"
        return [service.client for service in self._handlers.values() if (epa is None or service.client.handles( epa, **kwargs))]

    def getEpas(self) -> List[str]:
        epas = []
        for service in self._handlers.values():
            epas.extend( service.client.endpointSpecs )
        return epas

    def getClient( self, name: str, **kwargs ) -> StratusClient:
        service = self._handlers.get( name, None )
        assert service is not None, "Attempt to access unknown service handler: " + name + ", avaliable handlers = " + str( self._handlers.keys() )
        return service.client

    @property
    def available(self) -> Dict[str, Handler]:
        return self._handlers

    def __repr__(self):
        return json.dumps({key: s.parms for key,s in self._handlers.items()})

    def addConstructor( self, type: str, handler_constructor: Callable[[], Handler]  ):
        self.logger.info( "Adding constructor for " + type )
        self._constructors[type] = handler_constructor

    def listAvailableHandlers(self) -> List[str]:
        return list( self._constructors.keys() )

    def getHandler(self, service_spec: Dict[str,str] ) -> Handler:
        type = service_spec.get('type',None)
        name = service_spec.get('name', "")
        if type is None:
            raise Exception( "Missing required 'type' parameter in service spec'{}'".format(name) )
        constructor = self._constructors.get( type, None )
        assert constructor is not None, "No Handler registered of type '{}' for service spec '{}', handler types: {}".format( type, name, str(list(self._constructors.keys())) )
        return constructor( **service_spec )

    def listPackages(self):
        package = __import__("stratus")
        import pkgutil
        packages = []
        for loader, module_name, is_pkg in pkgutil.walk_packages(package.__path__, package.__name__ + '.'):
            if is_pkg: packages.append(module_name)
        return packages

    def addConstructors(self):
        debug = False
        packageList = self.listPackages()
        for package_name in packageList:
            try:
                module = importlib.import_module(package_name + ".service")
                constructor = getattr(module, "ServiceHandler")
                type = package_name.split(".")[-1]
                self.addConstructor( type, constructor )
            except Exception as err:
                if debug:
                    msg = "Unable to register constructor for {}: {} ({})".format( package_name, str(err), err.__class__.__name__ )
                    self.logger.warn( msg )
                    self.logger.warn( traceback.format_exc() )

handlers = Handlers()
