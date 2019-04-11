from stratus.app.core import StratusCore
import os
HERE = os.path.dirname(os.path.abspath(__file__))
SETTINGS_FILE = os.path.join( HERE, "settings.ini" )

if __name__ == "__main__":

    stratus = StratusCore( SETTINGS_FILE )
    server = stratus.getClient()

    variables=[ dict( name='tas:v0', uri='collection://merra2')  ]
    bounds = [ dict( axis='lat', start=-50, end=50, crs='values' ) ]
    domains=[ dict( name='d0', bounds=bounds) ]
    operations=[ dict( name='hpda.test1', axis='t', input='v0', domain='d0' )  ]
    request=dict(variables=variables, domains=domains, operations=operations )

    response = server.request( "exe", request=request )
    print( response )

    response = server.request( "stat", id=response['id'] )
    print( response )

    response = server.request( "kill", id=response['id'] )
    print( response )

