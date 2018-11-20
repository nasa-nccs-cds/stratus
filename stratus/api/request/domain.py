from .base import NamedObject

class Domain(NamedObject):

    def __init__(self, _name: str = None, **kwargs ):
        super(Domain, self).__init__(_name)