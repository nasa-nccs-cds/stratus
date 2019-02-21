import json, string, random, abc
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple
from stratus_endpoint.handler.base import Endpoint

class TestEndpoint(Endpoint):

    def __init__( self, **kwargs ):
        super(TestEndpoint, self).__init__( **kwargs )

    def request(cls, type: str, **kwargs ) -> Dict:
        return dict( type=type, result=kwargs )

    def init( cls ): pass

    @classmethod
    def getDefaultStatus(cls,  stat="executing", create=False ):
        if create: cls.current_exe_id = cls.randomStr(6)
        elif not  cls.current_exe_id: stat = "undefined"
        return  { "id": cls.current_exe_id, "status": stat }

class TestEndpoint1(TestEndpoint):

    def epas( cls ) -> List[str]:
        return [ "A", "B", "C", "D" ]

class TestEndpoint2(TestEndpoint):

    def epas( cls ) -> List[str]:
        return [ "C", "D", "E", "F" ]

class TestEndpoint3(TestEndpoint):

    def epas( cls ) -> List[str]:
        return [ "G", "H", "I", "J" ]

class TestEndpoint4(TestEndpoint):

    def epas( cls ) -> List[str]:
        return [ "A", "E", "J", "X" ]
