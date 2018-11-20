from .base import RequestObject
from .input import Input

class Operation(Input):

    def __init__(self, _name: str = None, inputs: ):
        super(Operation, self).__init__(_name)