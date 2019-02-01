from stratus.handlers.openapi.client import OpenApiClient

server = OpenApiClient( 'hpda1', server='127.0.0.1', port=5000, api='hpda1' )

variables=[ dict( name='tas:v0', uri='collection://merra2')  ]
bounds = [ dict( axis='lat', start=-50, end=50, crs='values' ) ]
domains=[ dict( name='d0', bounds=bounds) ]
operations=[ dict( name='ave', axis='t', input='v0', domain='d0' )  ]
request=dict(variables=variables, domains=domains, operations=operations )

response = server.request( "exe", request=request )
print( response )

response = server.request( "stat", id=response['id'] )
print( response )

response = server.request( "kill", id=response['id'] )
print( response )

