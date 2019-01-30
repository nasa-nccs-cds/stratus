from services.client import StratusClient

class DirectClient(StratusClient):

    def __init__( self, api: str, **kwargs ):
        super(DirectClient, self).__init__( api, **kwargs )