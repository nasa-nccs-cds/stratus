##stratus
#### Synchronization Technology Relating Analytic Transparently Unified Services

*Integrative framework presenting a unified API and workflow orchestration for varied climate data analytic services.*

Stratus is a workflow orchestration approach for incorporating varied earth data analytic services as a unified solution. This evolving framework provides a common interface and API across existing NCCS assets.

Coordinating workflows composed of services developed by disparate teams generally requires the combination of multiple orchestration strategies, e.g. fan-out, publish-subscribe, distributed task queue, and request-reply. In addition, multiple technologies are available for the implementation of each these strategies. Different technologies will be optimal with different contexts. The Stratus approach defines a common workflow and request API which is strategy and technology agnostic. It is composed of a set of orchestration nodes (i.e. self-contained building blocks) each implementing a particular composition strategy on a particular technology and designed to interface with other Stratus nodes. In this way an integration framework can be constructed by combining orchestration nodes like Lego blocks.

For example, a REST service might require a REST server (request-reply) located outside the firewall connected via zeroMQ (request-reply + pub-sub) through the firewall to Celery (distributed task queue) running on an analytics cluster. In Stratus these three layers are implemented as Stratus nodes which, because of the common API, can be easily combined as components in the overall service framework. Moving from one context to another context, e.g. from local cluster to the cloud, simply requires the replacement of cluster node(s) with equivalent cloud node(s), e.g. replacing a Stratus node implementing a ZeroMQ-based messaging service with one based on AWS SQS or Google gRPC.

### Installation

Conda environment setup:

 >> conda create -n stratus -c conda-forge python=3.6 libnetcdf netCDF4 pyyaml six xarray networkx requests decorator
 
Build Stratus installation by installing the stratus-endpoint and stratus packages:

    >> git clone https://github.com/nasa-nccs-cds/stratus-endpoint.git
    >> cd stratus-endpoint
    >> python setup.py install

    > git clone https://github.com/nasa-nccs-cds/stratus.git
    > cd stratus
    > python setup.py install <handlers(s)>

The handlers(s) qualifier in the last install command tells the builder to only install dependencies for the listed service handlers.  E.g. to build a service that supports both zeromq and rest one would execute *“python setup.py install zeromq rest”*, or for only rest: *“python setup.py install rest”*.  Simply executing *“python setup.py install”* would install the dependencies for all supported stratus service handlers.

The following are the currently available stratus service handlers: 
* endpoint
* zeromq
* openapi
* rest
* rest_client
* celery

##### Documentation

A whitepaper describing the Stratus framework is available at: https://www.dropbox.com/s/6ukb5917bv3r7df/STRATUS-WhitePaper-1.0.pdf?dl=0

##### Examples

In order to expose some capability within the Stratus framework, that capability must be wrapped as a Stratus endpoint.
Examples of Stratus endpoint wrappings can be found in the `stratus/handlers/endpoint/samples` directory.



