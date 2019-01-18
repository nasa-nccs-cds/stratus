from pyswagger import App, Security
from pyswagger.contrib.client.requests import Client
from pyswagger.spec.v2_0.objects import Operation
from stratus.util.parsing import HtmlCodec
# from stratus.client.base import Client
from pyswagger.utils import jp_compose

app = App._create_('http://127.0.0.1:5000/hpda/swagger.json')

client = Client()

variables=[ dict( name='tas:v0', uri='collection://merra2')  ]
domains=[ dict( name='d0', lat='-50,50')  ]
operations=[ dict( name='ave', input='v0', domain='d0' )  ]
request=dict(variables=variables, domains=domains, operations=operations )
app.mime_codec.register( "text/html", HtmlCodec() )
op: Operation = app.op['exe']
response = client.request( op(request=request) )

print( response.data )