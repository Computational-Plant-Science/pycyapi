# PlantIT Cluster [![Build Status](https://travis-ci.com/Computational-Plant-Science/plantit-cluster.svg?branch=master)](https://travis-ci.com/Computational-Plant-Science/plantit-cluster)

Workflow manager for PlantIT. Allows the PlantIT web platform to deploy jobs to compute resources.

**This project is under active development and is not yet stable.**

## Requirements

The following are required to run `plantit-cluster` in a Unix environment:

- Python 3.6+
- [Singularity](https://sylabs.io/docs/)
- [iRODS iCommands](https://wiki.cyverse.org/wiki/display/DS/Setting+Up+iCommands)

## Documentation

Full documentation can be found [here](https://plant-it-cluster.readthedocs.io/en/latest/).

Documentation can be built using:

```
cd docs
make html
```

## Installation

To install `plantit-cluster`, run:

```
pip3 install plantit-cluster
```

## Usage

`plantit-cluster` currently supports a run configuration: `plantit --job some.json --run [local]`. `dask-jobqueue` integration is in development.

## Tests

Before running tests, run `bootstrap.sh`, then bring test containers up with `docker-compose -f docker-compose.test.yml up` (`-d` for detached mode).

Unit tests can be run using: `docker-compose -f docker-compose.test.yml exec cluster pytest . -s`

Run integration tests with `docker-compose -f docker-compose.test.yml exec cluster /opt/plantit-cluster/integration-tests.sh`.
