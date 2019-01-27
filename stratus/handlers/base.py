import string, random, abc

class Handler:
    __metaclass__ = abc.ABCMeta

    @classmethod
    def randomStr(cls, length) -> str:
        tokens = string.ascii_uppercase + string.ascii_lowercase + string.digits
        return ''.join(random.SystemRandom().choice(tokens) for _ in range(length))

    @abc.abstractmethod
    def execute(self, request: str ): pass

    @abc.abstractmethod
    def status(self,  id: str ): pass

    @abc.abstractmethod
    def kill(self,  id: str ): pass
