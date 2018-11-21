from .api import Client

class ClientFactory:

    @classmethod
    def create( cls, id: str, **kwargs ) -> Client:
        pass