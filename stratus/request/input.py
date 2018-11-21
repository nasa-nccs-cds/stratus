from .base import RequestObject
from .domain import Domain

class Input(RequestObject):

    def __init__(self, _name: str = None, **kwargs ):
        self._domain: Domain = kwargs.get("domain", Domain.empty() )
        super(Input, self).__init__( _name, **kwargs )

    @property
    def domain(self) -> Domain:
        return self._domain if self._domain else Domain.empty()