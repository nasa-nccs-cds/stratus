from stratus.handlers.core import StratusCore
from stratus_endpoint.handler.base import Task

settings = dict( stratus = dict( type="rest", API="wps", host_address="https://edas.nccs.nasa.gov/wps/cwt" ) )
core = StratusCore(settings)
client = core.getClient()

edas_server_request = dict(
    domain=[{"name": "d0", "lat": {"start": 50, "end": 55, "system": "values"},
             "lon": {"start": 40, "end": 42, "system": "values"},
             "time": {"start": "1980-01-01", "end": "1981-12-31", "crs": "timestamps"}}],
    input=[{"uri": "collection://cip_cfsr_mth", "name": "tas:v0", "domain": "d0"}],
    operation=[{'name': "xarray.ave", 'axes': "t", "input": "v0"}]
)

task: Task = client.request(edas_server_request)
result = task.getResult()
print( "Got Result: " + str(result.header) )


