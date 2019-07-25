from stratus_endpoint.handler.base import TaskHandle, TaskResult
from typing import Sequence, List, Dict, Mapping, Optional, Any
from edas.process.test import TestDataManager as mgr
import time, xarray as xa
from stratus.app.core import StratusCore

USE_OPENDAP = True

if __name__ == "__main__":
    start = time.time()
    settings = dict(stratus=dict(type="zeromq", client_address="127.0.0.1", request_port="4566", response_port="4567"))
    stratus = StratusCore(settings)
    client = stratus.getClient()
    uri = mgr.getAddress("merra2", "tas") if USE_OPENDAP else "collection://cip_merra2_mth"

    requestSpec = dict(
        input=dict(uri=uri, name=f"tas"),
        operation=[ dict(name="xop:ave", axis="time") ]
    )

    task: TaskHandle = client.request(requestSpec)
    result: Optional[TaskResult] = task.getResult(block=True)

    print("\n\n")
    print(" Completed computation in " + str(time.time() - start) + " seconds")
    for dset in result.data:
        print(f"Received result dataset containing variables: ")
        for name, var in dset.data_vars.items():
            print( f"\t {name}:  dims = {var.dims}, shape = {var.shape}")
        rpath = f"/tmp/endpoint-sample-result.nc"
        print( f"Saving result to {rpath}")
        dset.to_netcdf( rpath )
    print("\n\n")
