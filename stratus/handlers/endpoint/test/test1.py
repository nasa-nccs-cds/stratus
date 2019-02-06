import json, string, random, abc
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple
from stratus.handlers.endpoint.base import Endpoint

class TestEndpoint1(Endpoint):
    __metaclass__ = abc.ABCMeta

    def __init__( self, **kwargs ):
        super(TestEndpoint1, self).__init__( **kwargs )

    def request(cls, epa: str, **kwargs ) -> Dict:
        return dict( result=kwargs )

    def epas( cls ) -> List[str]:
        return [ "hpda.test1" ]

    def init( cls ): pass

    @classmethod
    def getDefaultStatus(cls,  stat="executing", create=False ):
        if create: cls.current_exe_id = cls.randomStr(6)
        elif not  cls.current_exe_id: stat = "undefined"
        return  { "id": cls.current_exe_id, "status": stat }
