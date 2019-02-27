# stratus
##### *Synchronization Technology Relating Analytic Transparently Unified Services*

  Integrative framework presenting a unified API and workflow orchestration for varied climate data analytic services.

___
### Installation

Recommended: create and activate a python3 venv:
```
        >> python3 -m venv /path/to/ENV
        >> source /path/to/ENV/bin/activate
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
