from stratus_endpoint.handler.base import TaskHandle, TaskResult
from typing import Sequence, List, Dict, Mapping, Optional, Any
from edas.process.test import TestDataManager as mgr
import xarray as xa
from stratus.app.core import StratusCore

USE_OPENDAP = False

if __name__ == "__main__":

    settings = dict(stratus=dict(type="zeromq", client_address="127.0.0.1", request_port="4556", response_port="4557"))
    stratus = StratusCore(settings)
    client = stratus.getClient()

    time_range = {"start": "1980-01-01", "end": "2001-12-31", "crs": "timestamps"}
    uri = mgr.getAddress("merra2", "tas") if USE_OPENDAP else "collection://cip_merra2_mth"

    requestSpec = dict(
        domain=[dict(name=f"d0", time=time_range) ],
        input=[dict(uri=uri, name=f"tas:v0", domain=f"d0") ],
        operation=[dict(name="xop:ave", axis="xy", input=f"v0") ]
    )

    task: TaskHandle = client.request(requestSpec)
    result: Optional[TaskResult] = task.getResult(block=True)
    dsets: List[xa.Dataset] = result.data
    for index, dset in enumerate(dsets):
        fileName = f"/tmp/xops_endpoint_test_result-{index}.nc"
        print(f"Got result[{index}]: Saving to file {fileName} ")
