from app.core import StratusCore
import os

testEndpoint = dict( type="endpoint", module="stratus_endpoint.handler.test", object="TestEndpoint" )

if __name__ == "__main__":

    settings = dict( stratus=dict(type="test"), test1=testEndpoint, test2=testEndpoint, test3=testEndpoint )
    stratus = StratusCore( settings )
    app = stratus.getApplication()

    operation= [ dict( name='test1:op', result="r1", workTime="3.0" ),
                 dict( name='test2:op', result="r2", workTime="6.0" ),
                 dict( name='test3:op', input=["r1","r2"], result="r3", workTime="1.0" )   ]
    request=dict( operation=operation, rid="R0", cid="C0" )

    wFuture = app.processWorkflow( request )
