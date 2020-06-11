#!/usr/bin/env python

from setuptools import setup

setup(name='plantit-cluster',
      version='0.0.2',
      description='PlantIT workflow management API.',
      author='Computational Plant Science Lab',
      author_email='wbonelli@uga.edu',
      url='https://github.com/Computational-Plant-Science/plantit-cluster',
      packages=['plantit_cluster'],
      entry_points={
          'console_scripts': [
              'plantit = plantit_cluster.cluster:cli'
          ]
      },
      python_requires='>=3.6.9',
      install_requires=['requests', 'python-irodsclient', 'dask', 'dask-jobqueue', 'dagster', 'dagster-dask', 'pyaml'],
      setup_requires=['wheel'],
      tests_require=['pytest'])
