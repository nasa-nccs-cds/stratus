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
To install using pip:
```
    > pip install stratus
```    
Or (build from source):
```
    > git clone https://github.com/nasa-nccs-cds/stratus.git
    > cd stratus
    > python setup.py install

```
       
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
