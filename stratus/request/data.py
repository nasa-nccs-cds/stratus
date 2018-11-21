from .base import RequestObject
from .input import Input
from typing import  List, Dict, Any, Sequence, Union, Optional, ValuesView, Tuple
from enum import Enum, auto
from stratus.util.parsers import RequestParser
from .domain import Domain

class SourceType(Enum):
    UNKNOWN = auto()
    uri = auto()
    collection = auto()
    dap = auto()
    file = auto()
    archive = auto()

class DataSource:

    # @classmethod
    # def new(cls, variableSpec: Dict[str, Any] ):
    #     for type in SourceType:
    #        spec = variableSpec.get( type.name, None )
    #        if spec is not None:
    #             return DataSource( spec, type )
    #     raise Exception( "Can't find data source in variableSpec: " + str( variableSpec ) )

    def __init__(self, address: str,  type: SourceType = SourceType.UNKNOWN ):
        self.processUri( type, address )

    def processUri( self, stype: SourceType, _address: str ):
        if stype.name.lower() == "uri":
            toks = _address.split(":")
            scheme = toks[0].lower()
            if scheme == "collection":
                self.type = SourceType.collection
                self.address =  toks[1].strip("/")
            elif scheme.startswith("http"):
                self.type = SourceType.dap
                self.address = _address
            elif scheme == "file":
                self.type = SourceType.file
                self.address = toks[1]
            elif scheme == "archive":
                self.type = SourceType.archive
                self.address = toks[1]
            else:
                raise Exception( "Unrecognized scheme '{}' in url: {}".format(scheme,_address) )
        else:
            self.type = stype
            self.address = _address

    def __str__(self):
        return "DS({})[ {} ]".format( self.type.name, self.address )

class VID:

   def __init__(self, _name: str, _id: str ):
        self.name = _name
        self.id = _id

   def elem(self) -> Tuple[str,str]: return ( self.name, self.id  )

   def identity(self) -> bool: return ( self.name == self.id  )

   def __str__(self):
        return "{}:{}".format( self.name, self.id )

class DataInput(Input):

    # @classmethod
    # def new(cls, variableSpec: Dict[str, Any] ):
    #     vids = RequestParser.get( ["name", "id"], variableSpec )
    #     assert vids is not None, "Missing 'name' or 'id' parm in variableSpec: " + str(variableSpec)
    #     varnames = vids.split(",")
    #     vars = []
    #     for varname in varnames:
    #         nameToks = RequestParser.split( ["|", ":"], varname )
    #         name = nameToks[0]
    #         id = nameToks[-1]
    #         vars.append(VID(name, id))
    #     domain = variableSpec.get("domain")
    #     source = DataSource.new( variableSpec )
    #     return DataInput(vars, domain, source, variableSpec)

    def __init__(self, name: str, vars: List[VID], source: DataSource, domain: Domain, **kwargs ):
        super(DataInput, self).__init__(name,domain)
        self.vids: List[VID] = vars
        self.dataSource: DataSource = source
        self.metadata = kwargs

    def name2id(self, _existingMap: Dict[str,str] = None ) -> Dict[str,str]:
        existingMap = _existingMap if _existingMap is not None else {}
        existingMap.update( { v.elem() for v in self.vids if not v.identity() } )
        return existingMap

    def names(self) -> List[str]:
        return [ v.name for v in self.vids ]

    @property
    def ids(self) -> List[str]:
        return [ v.id for v in self.vids ]

    def providesId(self, vid: str ) -> bool:
        return vid in self.ids

    def getId(self) -> str:
        return ":".join(self.ids)

    def __str__(self):
        return "V({})[ domain: {}, source: {} ]".format( ",".join([str(v) for v in self.vids]), self.domain.name, str(self.dataSource))

class DataInputManager:

    # @classmethod
    # def new(cls, variableSpecs: List[Dict[str, Any]] ):
    #     vsources = [DataInput.new(variableSpec) for variableSpec in variableSpecs]
    #     vmap = {}
    #     for vsource in vsources:
    #         for var in vsource.vids:
    #             vmap[var.id] = vsource
    #     return DataInputManager( vmap )

    def __init__(self, _variables: Dict[str, DataInput]):
        self.variables: Dict[str, DataInput] = _variables

    def getDataInput( self, id: str ) -> DataInput:
        return self.variables.get( id )

    def getDataSources(self) -> ValuesView[DataInput]:
        return self.variables.values()

    def __str__(self):
        return "DataInput[ {} ]".format( ";".join( [ str(v) for v in self.variables.values() ] ) )
