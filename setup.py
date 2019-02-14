import sys, os
from setuptools import setup, find_packages
os.environ["AIRFLOW_GPL_UNIDECODE"] = "true"

with open('requirements.txt') as f:
    deps = [dep for dep in f.read().split('\n') if dep.strip() != '' and not dep.startswith('-e')]
    install_requires = deps

setup(name='stratus',
      version='1.0',
      description='Stratus: Integrative framework presenting a unified API and workflow orchestration for varied climate data analytic services',
      author='Thomas Maxwell',
      author_email='thomas.maxwell@nasa.gov',
      url='https://github.com/nasa-nccs-cds/stratus.git',
      packages=find_packages(),
      package_data={'stratus': ['api/*.yaml']},
      zip_safe=False,
      include_package_data=True,
      install_requires=install_requires)
