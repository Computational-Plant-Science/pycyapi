#!/usr/bin/env python

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name='plantit-cluster',
    version='0.0.9',
    description='PlantIT workflow management CLI.',
    long_description=long_description,
    long_description_content_type="text/markdown",
    author='Computational Plant Science Lab',
    author_email='wbonelli@uga.edu',
    license='BSD-3-Clause',
    url='https://github.com/Computational-Plant-Science/plantit-cluster',
    packages=setuptools.find_packages(),
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'plantit = plantit_cluster.cluster:cli'
        ]
    },
    python_requires='>=3.6.9',
    install_requires=['requests', 'python-irodsclient', 'dask', 'dask-jobqueue', 'dagster', 'dagster-dask',
                      'pyaml'],
    setup_requires=['wheel'],
    tests_require=['pytest'])
