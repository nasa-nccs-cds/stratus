from stratus.app.core import StratusCore
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Optional
from stratus_endpoint.handler.base import TaskHandle
import os

testEndpoint = dict( type="endpoint", module="stratus.handlers.endpoint.test", object="TestEndpoint1" )

if __name__ == "__main__":

    settings = dict( stratus=dict(type="test"), test1=testEndpoint, test2=testEndpoint, test3=testEndpoint )
    stratus = StratusCore( settings )
    app = stratus.getApplication()

    operation= [ dict( name='test1:op', result="r1", cid = "C0", workTime="3.0" ),
                 dict( name='test2:op', result="r2", cid = "C1", workTime="6.0" ),
                 dict( name='test3:op', input=["r1","r2"], result="r3", cid = "C2", workTime="1.0" )   ]
    request=dict( operation=operation, rid="R0", cid="C0" )

    app.submitWorkflow(request)
    for taskHandle in taskHandles.values():
        result = taskHandle.getResult()
        print( result )
