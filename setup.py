#!/usr/bin/env python

from distutils.core import setup

setup(name='stratus',
      version='1.0',
      description='Stratus: Integrative framework presenting a unified API and workflow orchestration for varied climate data analytic services',
      author='Thomas Maxwell',
      author_email='thomas.maxwell@nasa.gov',
      url='https://github.com/nasa-nccs-cds/stratus.git',
      packages=[ 'stratus', 'stratus.client', 'stratus.util', 'stratus.client.request' ]
)
