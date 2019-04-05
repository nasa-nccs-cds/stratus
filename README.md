# stratus
##### *Synchronization Technology Relating Analytic Transparently Unified Services*

  Integrative framework presenting a unified API and workflow orchestration for varied climate data analytic services.

___

Stratus is a workflow orchestration approach for incorporating varied earth data analytic services as a unified solution.  This evolving framework provides a common interface and API across existing NCCS assets. 

Coordinating workflows composed of services developed by disparate teams generally requires the combination of multiple orchestration strategies, e.g. fan-out, publish-subscribe, distributed task queue, and request-reply.  In addition, multiple technologies are available for the implementation of each these strategies.   Different technologies will be optimal with different contexts.   The Stratus approach defines a common workflow and request API which is strategy and technology agnostic.  It is composed of a set of orchestration nodes (i.e. self-contained building blocks) each implementing a particular composition strategy on a particular technology and designed to interface with other Stratus nodes.   In this way an integration framework can be constructed by combining orchestration nodes like Lego blocks.  

For example, a REST service might require a REST server (request-reply) located outside the firewall connected via zeroMQ (request-reply + pub-sub) through the firewall to Celery (distributed task queue) running on an analytics cluster.   In Stratus these three layers are implemented as Stratus nodes which, because of the common API,  can be easily combined as components in the overall service framework.    Moving from one context to another context, e.g. from local cluster to the cloud, simply requires the replacement of cluster node(s) with equivalent cloud node(s), e.g. replacing a Stratus node implementing a Celery-based distributed task queue with one based on Apache Lambda or Google gRPC. 

___

### Installation

##### Conda environment setup

```
 >> conda create -n stratus -c conda-forge python=3.6 libnetcdf netCDF4 pyyaml six xarray
 ```

Build Stratus installation by installing the *stratus-endpoint* and *stratus* packages:
```
    >> git clone https://github.com/nasa-nccs-cds/stratus-endpoint.git
    >> cd stratus-endpoint
    >> python setup.py install

    > git clone https://github.com/nasa-nccs-cds/stratus.git
    > cd stratus
    > python setup.py install <handlers(s)>
```
The handlers(s) qualifier in the last install command tells the builder to only install dependencies for the listed service handlers.  E.g. to build a service that supports both zeromq and rest one would execute *“python setup.py install zeromq rest”*, or for only rest: *“python setup.py install rest”*.  Simply executing *“python setup.py install”* would install the dependencies for all supported stratus service handlers.

The following are the currently available stratus service handlers: 
* endpoint
* zeromq
* openapi
* rest
* celery
* lambda


     
### Configuration
The stratus configuration file is located by ~/.stratus/settings.ini.  A sample can be found at stratus/settings.ini

### Defining a service

Stratus enables easy construction of Open API microservices.  Defining a service api involves the following steps:
    
1. Create a service.yml file adhering to the Open API 2.0 spec.  A sample can be found at stratus/api/hpda1.yml
2. Create a service endpoints package.  A sample can be found at stratus/endpoints/hpda1
3. Configure the service by adding the api name and the handler package to the settings.ini file
4. Start up the service by executing stratus/app.py
5. A sample python client can be found at stratus/client/hpda.py
6. A web interface to the service can be accessed at <servicePath>/ui, e.g. http://localhost:5000/hpda1/ui
