from stratus.handlers.openapi.client import OpenApiClient
from stratus.handlers.manager import handlers

server = handlers.getClient( "rest-hpda1" )

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

