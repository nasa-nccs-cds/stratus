import string, random, abc, os, yaml, json
from typing import List, Dict, Any, Sequence, Callable, BinaryIO, TextIO, ValuesView, Optional
from stratus.handlers.client import StratusClient
from stratus.util.config import Config, StratusLogger, UID
from stratus.handlers.app import StratusFactory
import itertools, traceback
import importlib

class Handlers:
    HERE = os.path.dirname( __file__ )
    STRATUS_ROOT = os.path.dirname( os.path.dirname( HERE ) )

    def __init__(self, settings: Dict[str,Dict], **kwargs ):
        self.logger = StratusLogger.getLogger()
        self._handlers: Dict[str, StratusFactory] = { }
        self._app_handler: StratusFactory = None
        self._parms = kwargs
        self._constructors: Dict[str, Callable[[], StratusFactory]] = {}
        self.configSpec: Dict[str,Dict] = settings
        self._init()

    def _init( self ):
        self._addConstructors()
        hspecs = self._getHandlerSpecs()
        for service_spec in hspecs:
            htype = service_spec["type"]
            try:
                service = self._getHandler(service_spec)
                if service.name == "stratus":
                    self._app_handler = service
                else:
                    self._handlers[ service.name ] = service
                    self.logger.info( f"Initialized stratus for service {htype}" )
            except Exception as err:
                err_msg = "Error registering handler for service {}: {}".format( service_spec.get("name",""), str(err) )
                print( err_msg )
                self.logger.error( err_msg )

    def __getitem__( self, key: str ) -> StratusFactory:
        result =  self._handlers.get(key, None)
        assert result is not None, "Attempt to access unknown handler in Handlers: {} ".format( key )
        return result

    def getClients( self, epa: str = None, **kwargs ) -> List[StratusClient]:
        assert self.configSpec is not None, "Error, the handlers have not yet been initialized"
        return [service.client() for service in self._handlers.values() if (epa is None or service.client().handles( epa, **kwargs))]

    def getEpas(self) -> List[str]:
        epas = []
        for service in self._handlers.values():
            epas.extend( service.client().endpointSpecs )
        return epas

    def getApplicationHandler(self) -> Optional[StratusFactory]:
        return self._app_handler

    def findHandler(self, **kwargs ) -> Optional[StratusFactory]:
        name = kwargs.get( "service_name", kwargs.get( "name", None ) )
        if name is not None:
            return self._handlers.get( name, None )
        type = kwargs.get("service_type", kwargs.get("type", None ) )
        if type is  None: raise Exception( "Missing handler specification, must have 'service_name' or 'service_type' parameter in [stratus] ini configuration: " + str(kwargs))
        for handler in self._handlers.values():
            if handler.type == type:
                return handler
        return None

    @property
    def available(self) -> Dict[str, StratusFactory]:
        return self._handlers

    def listAvailableHandlers(self) -> List[str]:
        return list( self._constructors.keys() )

    def __repr__(self):
        return json.dumps({key: s.parms for key, s in self._handlers.items()})

    def _getHandlerSpecs(self) -> List[Dict[str,str]]:
        specs = []
        htypes = self.listAvailableHandlers()
        assert self.configSpec is not None, "Error, the handlers have not yet beeb initialized"
        for name, spec in self.configSpec.items():
            if (spec.get("type") in htypes):
                spec["name"] = name
                specs.append( spec )
        return specs

    def _addConstructor(self, type: str, handler_constructor: Callable[[], StratusFactory]):
        self.logger.info( "Adding constructor for " + type )
        self._constructors[type] = handler_constructor

    def _getHandler(self, service_spec: Dict[str,str] ) -> StratusFactory:
        type = service_spec.get('type',None)
        name = service_spec.get('name', "")
        if type is None:
            raise Exception( "Missing required 'type' parameter in service spec'{}'".format(name) )
        constructor = self._constructors.get( type, None )
        assert constructor is not None, "No Handler registered of type '{}' for service spec '{}', handler types: {}".format( type, name, str(list(self._constructors.keys())) )
        return constructor( **service_spec )

    def _listPackages(self) -> List[str]:
        package = __import__("stratus")
        import pkgutil
        packages = []
        for loader, module_name, is_pkg in pkgutil.walk_packages(package.__path__, package.__name__ + '.'):
            if is_pkg: packages.append(module_name)
        return packages

    def _addConstructors(self):
        packageList = self._listPackages()
        for package_name in packageList:
            try:
                module = importlib.import_module(package_name + ".service")
                constructor = getattr(module, "ServiceHandler")
                type = package_name.split(".")[-1]
                self._addConstructor(type, constructor)
            except ModuleNotFoundError as err:
                self.logger.warn( "No handler found for path: " + package_name )
            except Exception as err:
                msg = "Unable to register constructor for {}: {} ({})".format( package_name, str(err), err.__class__.__name__ )
                self.logger.error( msg )
                self.logger.error( traceback.format_exc() )

