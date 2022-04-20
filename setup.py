#!/usr/bin/env python

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name='pycyapi',
    version='0.0.5',
    description='A Python client for the CyVerse Discovery Environment API (a.k.a. Terrain).',
    long_description=long_description,
    long_description_content_type="text/markdown",
    author='Computational Plant Science Lab',
    author_email='wbonelli@uga.edu',
    license='BSD-3-Clause',
    url='https://github.com/Computational-Plant-Science/pycyapi',
    packages=setuptools.find_packages(),
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'pycy = pycyapi.cli:cli'
        ]
    },
    python_requires='>=3.6.8',
    install_requires=['requests', 'httpx', 'click', 'tenacity', 'tqdm', 'pytest'],
    setup_requires=['wheel'],
    tests_require=['pytest', 'coveralls'])