from .base import RequestObject

class Input(RequestObject):

    def __init__(self, _name: str = None , _domain: str = None ):
        super(Input, self).__init__(_name)
        self.domain = _domain