import string, random, abc, os


class ServiceManager:
    HERE = os.path.dirname(__file__)
    SETTINGS = os.path.join( HERE, 'services.yml')

    def __init__(self):
        self.services = []

class Handler:
    __metaclass__ = abc.ABCMeta

    @classmethod
    def randomStr(cls, length) -> str:
        tokens = string.ascii_uppercase + string.ascii_lowercase + string.digits
        return ''.join(random.SystemRandom().choice(tokens) for _ in range(length))

    @abc.abstractmethod
    def handles(self, op: str )-> bool : pass

    @abc.abstractmethod
    def processRequest(self, op: str, **kwargs ): pass


class DebugHandler(Handler):

    def handles(self, op: str )-> bool : return True

    def processRequest(self, op: str, **kwargs ):
        rid = kwargs.get("id", self.randomStr(8) )
        kwargs.update( { "op" : op, "id": rid, "status": "complete" } )
        return kwargs

class Handlers:
    handlers = [ DebugHandler() ]

    @classmethod
    def processRequest(cls, op, **kwargs ):
        handler = cls.getHandler( op )
        return handler.processRequest( op, **kwargs )

    @classmethod
    def getHandler( cls, op: str ):
        for handler in cls.handlers:
            if handler.handles( op ):
                return handler
        return None

    @classmethod
    def addHandler(cls, handler ):
        cls.handlers.insert( 0, handler )
