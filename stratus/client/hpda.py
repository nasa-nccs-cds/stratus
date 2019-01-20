from pyswagger import App, Security
from pyswagger.contrib.client.requests import Client
from pyswagger.spec.v2_0.objects import Operation
from stratus.util.parsing import HtmlCodec
# from stratus.client.base import Client
from pyswagger.utils import jp_compose

app = App._create_('http://127.0.0.1:5000/hpda/swagger.json')

client = Client()

variables=[ dict( name='tas:v0', uri='collection://merra2')  ]
bounds = [ dict( axis='lat', start=-50, end=50, crs='values' ) ]
domains=[ dict( name='d0', bounds=bounds) ]
operations=[ dict( name='ave', axis='t', input='v0', domain='d0' )  ]
request=dict(variables=variables, domains=domains, operations=operations )

op: Operation = app.op['exe']
response = client.request( op(request=request) )
print( response.data )

op: Operation = app.op['exeStat']
response = client.request( op( id=response.data['id'] ) )
print( response.data )

op: Operation = app.op['exeKill']
response = client.request( op( id=response.data['id'] ) )
print( response.data )

