<div align="center">
<img src="de.png" style="position:relative;top: 75px;width:50px;" />

# PyCyAPI

unofficial Python/CLI client for the [CyVerse](https://www.cyverse.org/) [Science](https://cyverse.org/Science-APIs) (a.k.a. [Terrain](https://de.cyverse.org/terrain/docs/index.html)) APIs

[![PyPI Version](https://img.shields.io/pypi/v/pycyapi.png)](https://pypi.python.org/pypi/pycyapi)
[![PyPI Status](https://img.shields.io/pypi/status/pycyapi.png)](https://pypi.python.org/pypi/pycyapi)
[![PyPI Versions](https://img.shields.io/pypi/pyversions/pycyapi.png)](https://pypi.python.org/pypi/pycyapi)

![CI](https://github.com/Computational-Plant-Science/pycyapi/workflows/CI/badge.svg)
[![Documentation Status](https://readthedocs.org/projects/pycyapi/badge/?version=latest)](https://pycyapi.readthedocs.io/en/latest/?badge=latest)

</div>

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [Status](#status)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quickstart](#quickstart)
- [Documentation](#documentation)
- [Disclaimer](#disclaimer)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Status

API coverage is still sparse, focusing mainly on `data` and `fileio` endpoints. It will likely stay this way as this project exists predominantly for consumption by [`plantit`](https://github.com/Computational-Plant-Science/plantit) and [`plantit-action`](https://github.com/Computational-Plant-Science/plantit-action).

## Requirements

- Python 3.8+

## Installation

This project is [available on the Python Package Index](https://pypi.org/project/pycyapi/). To install with pip:

```shell
pip install pycyapi
```

## Quickstart

Generally, the CLI is invoked with `pycyapi <command>`. You will first need an authentication token. To obtain one, provide your CyVerse username and password to the `token` command:

```shell
pycyapi token --username <username> --password <password>
```

The token is printed and can be used with the `--token` option to authenticate subsequent commands.

**Note:** the `token` command is the only one to return plain text &mdash; all other commands return JSON.

Now, to list the contents of your home folder in the Data Store:

```shell
pycyapi list --path /iplant/home/<username> --token <token>
```

## Documentation

Documentation is available at [pycyapi.readthedocs.io](https://pycyapi.readthedocs.io/en/latest/).

## Disclaimer

This project is not affiliated with CyVerse and cannot guarantee compatibility.