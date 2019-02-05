from stratus.handlers.openapi.client import OpenApiClient
from stratus.handlers.manager import handlers

server = handlers.getClient( "rest-hpda1" )


operations=[ dict( name='ave', axis='t', input='v0', domain='d0' )  ]
request=dict(variables=variables, domains=domains, operations=operations )

response = server.request( "hpda.test1", request=request )
print( response )

response = server.request( "stat", id=response['id'] )
print( response )

response = server.request( "kill", id=response['id'] )
print( response )

