from stratus_endpoint.handler.base import TaskHandle, TaskResult
from typing import Sequence, List, Dict, Mapping, Optional, Any
from edas.process.test import TestDataManager as mgr
import time, xarray as xa
from stratus.app.core import StratusCore

USE_OPENDAP = True

if __name__ == "__main__":
    start = time.time()

#  Startup a Stratus zmq client and connect to a server on localhost

    settings = dict(stratus=dict(type="zeromq", client_address="127.0.0.1", request_port="4566", response_port="4567"))
    stratus = StratusCore(settings)
    client = stratus.getClient()

# Define an analytics request (time average of merra2 surface temperature) directed to the 'xop' endpoint

    uri = mgr.getAddress("merra2", "tas") if USE_OPENDAP else "collection://cip_merra2_mth"
    requestSpec = dict(
        input=dict(uri=uri, name=f"tas"),
        operation=[ dict(name="xop:ave", axis="time") ]
    )

# Submit the request to the server and wait for the result

    task: TaskHandle = client.request(requestSpec)
    result: Optional[TaskResult] = task.getResult(block=True)

# Print result metadata and save the result to disk as a netcdf file

    print("\n\nCompleted computation in " + str(time.time() - start) + " seconds")
    for ind, dset in enumerate(result.data):
        print(f"Received result dataset containing variables: ")
        for name, var in dset.data_vars.items():
            print( f"\t {name}:  dims = {var.dims}, shape = {var.shape}")
        rpath = f"/tmp/endpoint-sample-result-{ind}.nc"
        print( f"Saving result to {rpath}\n\n")
        dset.to_netcdf( rpath )

