import os, json, yaml, abc, itertools, queue
from typing import List, Union, Dict, Set, Iterator, Tuple, ItemsView
from stratus_endpoint.util.config import Config, StratusLogger
from multiprocessing import Process as SubProcess
from stratus.app.operations import *
from threading import Thread

class StratusCoreBase:
    HERE = os.path.dirname(__file__)
    SETTINGS = os.path.join( HERE, 'settings.ini')

    def __init__(self, configSpec: Union[str,Dict[str,Dict]], **kwargs ):
        self.logger = StratusLogger.getLogger()
        self.config = self.getSettings( configSpec )
        self.parms = self.getConfigParms('stratus')
        self.id = UID.randomId(8)

    @classmethod
    def getSettings( cls, configSpec: Union[str,Dict[str,Dict]] ) -> Dict[str,Dict]:
        result = {}
        if isinstance(configSpec, str ):
            assert os.path.isfile(configSpec), "Settings file does not exist: " + configSpec
            if configSpec.endswith( ".ini" ):
                config =  Config(configSpec)
                for section in config.sections():
                    result[section] = config.get_map( section )
            elif ( configSpec.endswith( ".yml" ) or configSpec.endswith( ".yaml" ) ):
                with open(configSpec, 'r') as stream:
                    result =  yaml.load(stream)
        else:
            result = configSpec
        return result

    def getConfigParms(self, module: str ) -> Dict:
        return self.config.get( module, {} )

    def parm(self, name: str, default = None ) -> str:
        parm = self.parms.get( name, default )
        if parm is None: raise Exception( "Missing required stratus parameter in settings.ini: " + name )
        return parm

    def __getitem__( self, key: str ) -> str:
        result =  self.parms.get( key, None )
        assert result is not None, "Missing required parameter in {}: {} ".format( self.__class__.__name__, key )
        return result

    def getCapabilities(self, ctype ) -> Dict:
        caps = {}
        for client in self.getClients():
            self.logger.info( f"GET Capabilites from client: {client.__class__.__name__}, name = {client.name}, cid = {client.cid}")
            caps.update( client.capabilities(ctype) )
        return caps

    @abc.abstractmethod
    def getClients( self, op: Op = None, **kwargs ) -> List[StratusClient]: pass

    @abc.abstractmethod
    def getClient( self, **kwargs ) -> StratusClient: pass

    @abc.abstractmethod
    def getApplication( self ) -> "StratusAppBase": pass

    @abc.abstractmethod
    def getEpas( self,  **kwargs ) -> List[str]: pass

    @property
    @abc.abstractmethod
    def internal_clients(self): pass


class StratusAppBase(Thread):
    __metaclass__ = abc.ABCMeta

    def __init__( self, _core: StratusCoreBase, **kwargs ):
        Thread.__init__( self )
        self.logger = StratusLogger.getLogger()
        self.core = _core
        self.registeredRequests = set()
        self.requestQueue = queue.Queue()
        self.active_workflows: Dict[str, StratusWorkflow] = {}
        self.completed_workflows: Dict[str, StratusWorkflow] = {}
        self._active = True

    @property
    def internal_clients(self):
        return self.core.internal_clients

    def run(self):
        self.logger.info(" &&&&&&&&&&&&&&&&&&&&&&&&& Running STRATUS App: " + self.__class__.__name__ + " &&&&&&&&&&&&&&&&&&&&&&&&&")
        self.initInteractions()
        self.logger.info(" &&&&&&&&&&&&&&&&&&&&&&&&& Starting STRATUS App Loop &&&&&&&&&&&&&&&&&&&&&&&&&")
        while self._active:
            self.ingestRequests()
            self.update_workflows()
            self.updateInteractions()
            time.sleep(0)

    @abc.abstractmethod
    def processError(self, rid: str, ex: Exception): pass

    @abc.abstractmethod
    def initInteractions(self): pass

    @abc.abstractmethod
    def updateInteractions(self): pass

    def distributeOps(self, clientOpsets: Dict[str, ClientOpSet]) -> Iterator[ClientOpSet]:
        # Distributes ops to clients while maximizing locality of operations
        filtered_opsets: Set[ClientOpSet] = set()
        processed_ops: List[str] = []
        sorted_opsets: List[ClientOpSet] = list(sorted(clientOpsets.items(), reverse=True, key=lambda x: x[1]))
        while len( sorted_opsets ):
            cid, base_opset = sorted_opsets.pop(0)
            new_opset = base_opset.new()
            for op in base_opset:
                if op.id not in processed_ops:
                    processed_ops.append( op.id )
                    new_opset.add( op )
            if len( new_opset ) > 0:
                filtered_opsets.add( new_opset )
            for cid, opset in sorted_opsets:
                opset.remove( processed_ops )
            sorted_opsets = list( sorted( sorted_opsets, reverse=True, key=lambda x: x[1] ) )
        distributed_opsets = [opset.connectedOpsets() for opset in filtered_opsets]
        return itertools.chain.from_iterable(distributed_opsets)

    def submitWorkflow(self, request: Dict):
        request.setdefault("rid", UID.randomId(6))
        self.requestQueue.put( request )
        self.registeredRequests.add( request["rid"] )
        return request

    def createWorkflow( self, tasks: List[WorkflowTask] ) -> StratusWorkflow:
        return StratusWorkflow(nodes=tasks)

    def ingestRequests( self ):
        rid = ""
        while True:
            try:
                request = self.requestQueue.get_nowait()
                rid = request.get("rid", UID.randomId(6))
                self.logger.info(f"Ingest request: {rid}")
                clientOpsets: Dict[str, ClientOpSet] = self.geClientOpsets(request)
                tasks: List[WorkflowTask] = [WorkflowTask(cOpSet) for cOpSet in self.distributeOps(clientOpsets)]
                self.active_workflows[ rid ] = self.createWorkflow( tasks )
            except queue.Empty:
                return
            except Exception as err:
                msg =  f"Error ingesting request: {err}\n" + traceback.format_exc()
                self.logger.error(msg)
                self.logger.error( traceback.format_exc() )
                workflow = StratusWorkflow(error=(msg, err))
                self.active_workflows[ rid ] = workflow

    def update_workflows(self):
        completed_list = {}
        for rid, workflow in self.active_workflows.items():
            completed = workflow.update()
            if completed:
                self.logger.info(f" ***********************************   StratusApp.completed_workflow: {rid}")
                completed_list[rid] = workflow
        for rid, workflow in completed_list.items():
            self.completed_workflows[rid] = workflow
            del self.active_workflows[rid]

    def waitForCompletion(self, rid: str ):
        while( True ):
            self.update_workflows()
            if rid in self.completed_workflows: return
            time.sleep( 0.1 )

    def getResult( self, rid: str  ) -> Optional[TaskHandle]:
        workflow = self.completed_workflows.get( rid )
        return None if workflow is None else workflow.getResult()

    def getWorkflows(self) -> Dict[str, StratusWorkflow]:
        return { **self.completed_workflows, **self.active_workflows }

    def getWorkflowItems(self) -> ItemsView[str, StratusWorkflow]:
        workflows = self.getWorkflows()
        return workflows.items()

    def getWorkflow(self, rid: str) -> Optional[StratusWorkflow]:
        return self.getWorkflows().get(rid)

    def clearWorkflow(self, rid: str):
        if rid in self.completed_workflows:
            del self.completed_workflows[rid]
            try: self.registeredRequests.remove( rid )
            except Exception: pass
        else:
            self.logger.error( f"Attampt to clear a workflow {rid} that is not in the completed_workflows")

    def geClientOpsets(self, request: Dict ) -> Dict[str, ClientOpSet]:
        # Returns map of client id to list of ops in request that can be handled by that client
        ops: List[Dict] = request.get("operation")
        assert ops is not None, "Missing 'operation' parameter in request: " + str( request )
        clientOpsets: Dict[str, ClientOpSet] = dict()
        ops: List[Op] = OpSet( nodes = [ Op( **opDict ) for opDict in ops ] )
        for op in ops:
            clients = self.core.getClients( op )
            assert len(clients) > 0, f"Can't find a client to process the operation': {op.epas}, clients = { [str(client.endpointSpecs) for client in self.core.getClients( op )] }"
            for client in clients:
               opSet = clientOpsets.setdefault(client.handle, ClientOpSet(request,client))
               opSet.add( op )
        return clientOpsets

    def shutdown(self):
        self._active = False

    def parm(self, name: str, default = None ) -> str:
        return self.core.parm( name, default )

    def __getitem__( self, key: str ) -> str:
        return self.core[key]

    def getConfigParms(self, module: str ) -> Dict:
        return self.core.getConfigParms( module )

class StratusServerApp(StratusAppBase):
    __metaclass__ = abc.ABCMeta

    def __init__( self, core: StratusCoreBase, **kwargs ):
        StratusAppBase.__init__( self, core, **kwargs )

    def exec(self) -> SubProcess:
        proc = SubProcess( target=self.run )
        proc.start()
        return proc

class StratusEmbeddedApp(StratusAppBase):

    def __init__( self, core: StratusCoreBase ):
        StratusAppBase.__init__(self, core)
        self.logger =  StratusLogger.getLogger()
        self.parms = self.getConfigParms('stratus')

    def run(self):
        pass

    def init(self, **kwargs):
        pass

    def handle_client_request(self, requestSpec: Dict, inputs: List[TaskResult] = None, **kwargs) -> TaskHandle:
        rid = requestSpec["rid"]
        self.requestQueue.put( requestSpec )
        self.ingestRequests()
        self.update_workflows()
        self.waitForCompletion( rid )
        return self.getResult( rid )

    def capabilities(self, ctype: str, **kwargs ) -> Dict:
        return self.core.getCapabilities( ctype )

    def log(self, msg: str ):
        self.logger.info( "[ZP] " + msg )

class TestStratusApp(StratusServerApp):

    def initInteractions(self): return

    def updateInteractions(self): return

class StratusFactory:
    __metaclass__ = abc.ABCMeta

    def __init__(self, htype: str, **kwargs):
        self.logger = StratusLogger.getLogger()
        self.parms = kwargs
        self.name = self['name']
        self.type: str = htype
        htype1 = self.parms.pop("type")
        assert htype1 == htype, "Sanity check of Handler type failed: {} vs {}".format(htype1,htype)

    @abc.abstractmethod
    def client( self, core: StratusCoreBase, **kwargs ) -> StratusClient: pass

    @abc.abstractmethod
    def getClient( self, cid: str = None ) -> StratusClient: pass

    @abc.abstractmethod
    def app(self, core: StratusCoreBase ) -> StratusAppBase: pass

    @abc.abstractmethod
    def buildWorker(self, name: str, spec: Dict[str, str]): pass

    def __getitem__( self, key: str ) -> str:
        result =  self.parms.get( key, None )
        assert result is not None, "Missing required parameter in {}: {} ".format( self.__class__.__name__, key )
        return result

    def parm(self, key: str, default: str) -> str:
        return self.parms.get(key, default)

    def __repr__(self):
        return json.dumps(self.parms)

    def __str__(self):
        return f"SF[{self.name}:{self.type}]"

