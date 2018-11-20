# stratus :sparkles:
##### *Synchronization Technology Relating Analytic Transparently Unified Subsystems*

  Integrative framework presenting a unified API and workflow orchestration for varied climate data analytic services.

___
### Installation

###### Building a Conda environment for high performance climate data analytics
```
    > conda create -n hpcda -c conda-forge -c cdat python=3.6 cdms2 cdutil cdtime
    > source activate hpcda
    > conda install  -c conda-forge libnetcdf nco eofs dask distributed xarray matplotlib scipy bottleneck paramiko netCDF4 defusedxml python-graphviz bokeh pyparsing pillow scikit-learn tensorflow keras zeromq pyzmq pytest cartopy paramiko
    > pip install pydap sklearn_xarray
```


###### Installing Stratus
```
    > git clone https://github.com/nasa-nccs-cds/stratus.git
    > cd stratus
    > python setup.py install

```
