from stratus.client.base import ServerProxy

server = ServerProxy( 'http://127.0.0.1:5000/hpda/swagger.json' )

variables=[ dict( name='tas:v0', uri='collection://merra2')  ]
bounds = [ dict( axis='lat', start=-50, end=50, crs='values' ) ]
domains=[ dict( name='d0', bounds=bounds) ]
operations=[ dict( name='ave', axis='t', input='v0', domain='d0' )  ]
request=dict(variables=variables, domains=domains, operations=operations )

response = server.request( 'exe', request=request )
print( response )

response = server.request( 'exeStat', id=response.data['id'] )
print( response )

response = server.request( 'exeKill', id=response.data['id'] )
print( response )

