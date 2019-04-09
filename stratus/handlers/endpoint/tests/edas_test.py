from stratus.handlers.endpoint.client import DirectClient
from stratus_endpoint.handler.base import TaskHandle, Status

if __name__ == "__main__":
    CreateIPServer = "https://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/"

    addresses = {
        "merra2": CreateIPServer + "/reanalysis/MERRA2/mon/atmos/{}.ncml",
        "merra": CreateIPServer + "/reanalysis/MERRA/mon/atmos/{}.ncml",
        "ecmwf": CreateIPServer + "/reanalysis/ECMWF/mon/atmos/{}.ncml",
        "cfsr": CreateIPServer + "/reanalysis/CFSR/mon/atmos/{}.ncml",
        "20crv": CreateIPServer + "/reanalysis/20CRv2c/mon/atmos/{}.ncml",
        "jra": CreateIPServer + "/reanalysis/JMA/JRA-55/mon/atmos/{}.ncml"
    }

    def getAddress( model: str, varName: str) -> str:
        return addresses[model.lower()].format(varName)


    client = DirectClient(module="edas.stratus.endpoint", handler="EDASEndpoint")
    client.init()

    request = dict(
        domain = [{"name": "d0", "lat": {"start": 50, "end": 55, "system": "values"}, "lon": {"start": 40, "end": 42, "system": "values"},  "time": {"start": "1980-01-01", "end":  "1981-12-31", "crs": "timestamps"} } ],
        input = [ {"uri": getAddress("merra2", "tas"), "name": "tas:v0", "domain": "d0"} ],
        operation = [ { "name": "xarray.subset", "input": "v0" } ]
    )
    task: TaskHandle = client.request( "exe", **request )
    result = task.getResult( block = True )
    print ( "Received result: " + str(result) )


