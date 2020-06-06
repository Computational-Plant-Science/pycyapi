#!/usr/bin/env python

from distutils.core import setup

setup(name='plantit-cluster',
      version='0.0.1',
      description='PlantIT workflow management API.',
      author='Computational Plant Science Lab',
      author_email='wbonelli@uga.edu',
      url='https://github.com/Computational-Plant-Science/plantit-cluster',
      packages=['cluster'],
      entry_points={
        'console_scripts':[
            'plantit = cluster.cluster:cli'
        ]
      },
      install_requires=['requests','python-irodsclient', 'dask', 'dask-jobqueue', 'dagster', 'dagster-dask'],
      tests_require=['pytest']
     )
