from uuid import uuid4 as uuid

class RequestObject:

    def __init__(self, _name: str = None ):
        self.name = _name if _name else str(uuid())[30:]