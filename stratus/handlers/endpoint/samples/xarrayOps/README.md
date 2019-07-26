## XarrayOps Endpoint Sample 

Endpoints are used to define and deploy new analytic services in **STRATUS**.

##### Endpoint implementation example

To create an new Endpoint one must define two classes, an `ExecEndpoint` subclass, 
which implements the endpoint, and an `Executable` subclass, which implements a single operation.
A sample implementation with documentation can be found in the *endpoint.py* file.

##### Endpoint deployment example

An example of endpoint deployment is found in the *test* subdirectory.  The *zmq_server.py* illistrates a deployment of the
**XarrayOps** endpoint as a ZeroMQ analytics service.  The deployment parameters, defined in the *zmq_server_settings.ini*
file, specify the python package and class of the endpoint.  The *zmq_client.py* file illustrates the implementation of
a client which invokes the service and then saves the result to disk.