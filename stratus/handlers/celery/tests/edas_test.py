from stratus_endpoint.handler.base import TaskHandle, TaskResult
from typing import Sequence, List, Dict, Mapping, Optional, Any
from edas.process.test import TestDataManager as mgr
import os, xarray as xa
from stratus.app.core import StratusCore
HERE: str = os.path.dirname(os.path.abspath(__file__))
SETTINGS_FILE: str = os.path.join( HERE, "edas_test_settings.ini" )

if __name__ == "__main__":

    stratus = StratusCore( SETTINGS_FILE )
    client = stratus.getClient()
    uri =  mgr.getAddress("merra2", "tas")

    requestSpec = dict(
        domain=[ dict( name="d0", time={"start": "1980-01-01", "end": "2001-12-31", "crs": "timestamps"} )  ],
        input=[ dict( uri=uri, name="tas:v0", domain="d0" ) ],
        operation=[ dict( name="edas:ave", axis="xy", input="v0", result="r0" ),  dict( name="demo:log", input="r0" )  ]
    )

    task: TaskHandle = client.request( requestSpec )
    result: Optional[TaskResult] = task.getResult( block=True )
    dsets: List[xa.Dataset] = result.data
    print(f"Completed Request, NResults = {len(dsets)}" )
    dvar: xa.Variable
    for index,dset in enumerate(dsets):
        fileName =  f"/tmp/edas_endpoint_test_result-{index}.nc"
        print( f"Got result[{index}]: Saving to file {fileName}, Variables: " )
        for vname in dset.variables:
            print(f"   -> Variable[{vname}]: Shape = {dset[vname].shape}")
        dset.to_netcdf( fileName )

