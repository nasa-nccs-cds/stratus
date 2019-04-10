import sys, os
from setuptools import setup, find_packages

def get_requirements():
    handlers = [ "celery", "endpoint", "zeromq", "openapi", "lambda", "rest", "rest_client" ]
    requirement_files = []
    for handler in handlers:
        if handler in sys.argv:
            sys.argv.remove(handler)
            requirement_files.append( f"requirements/{handler}.txt" )
    if len(requirement_files) == 0:
        requirement_files = [ f"requirements/{handler}.txt" for handler in handlers ]
    return requirement_files

install_requires = set()
for requirement_file in get_requirements():
    with open( requirement_file ) as f:
        for dep in f.read().split('\n'):
            if dep.strip() != '' and not dep.startswith('-e'):
                install_requires.add( dep )

print( "Installing with dependencies: " + str(install_requires) )

setup(name='stratus',
      version='1.0',
      description='Stratus: Integrative framework presenting a unified API and workflow orchestration for varied climate data analytic services',
      author='Thomas Maxwell',
      author_email='thomas.maxwell@nasa.gov',
      url='https://github.com/nasa-nccs-cds/stratus.git',
      packages=find_packages(),
      package_data={ 'stratus': ['api/*.yaml'], 'stratus.handlers.rest.api.wps': ['templates/*.xml'] },
      zip_safe=False,
      include_package_data=True,
      install_requires=list(install_requires))
