from uuid import uuid4 as uuid

class RequestObject:

    def __init__(self, objId: str = None, **kwargs ):
        self._id = objId if objId else str(uuid())[30:]
        self._properties = kwargs

    @property
    def id(self) -> str:
        return self._id

    def __getitem__( self, key: str ): return self._properties.get( key )

    def __getattr__(self, key: str ):
         return self._properties.get( key )