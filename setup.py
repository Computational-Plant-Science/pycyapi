#!/usr/bin/env python

from distutils.core import setup

setup(name='clusterside',
      version='0.0.1',
      description='Bridge between the DIRT2 web platform and the cluster.',
      author='Chris Cotter',
      author_email='cotter@uga.edu',
      url='dirt.cyverse.com',
      packages=['clusterside'],
      entry_points={
        'console_scripts':[
            'clusterside = clusterside.clusterside:cli'
        ]
      },
      requires=['requests','python-irodsclient']
     )
