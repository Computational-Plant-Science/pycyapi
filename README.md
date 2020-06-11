# PlantIT Cluster [![Build Status](https://travis-ci.com/Computational-Plant-Science/plantit-cluster.svg?branch=master)](https://travis-ci.com/Computational-Plant-Science/plantit-cluster)

PlantIT workflow management CLI. Executes jobs on local or distributed compute resources.

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

To execute a job defined in `job.json`, run `plantit --job job.json`. The `job.json` definition should conform to the following schema:

```json
{
    "id": "2",
    "workdir": "/",
    "token": "token",
    "server": "",
    "container": "docker://alpine:latest",
    "commands": "/bin/ash -c 'pwd'",
    "executor": {
        "name": "local"
    }
}
```

`plantit-cluster` currently supports `local` and `slurm` executors. A `pbs` executor is in development. If no executor is specified in the job definition file, `plantit-cluster` will default to the `local` (in-process) executor.

To use the SLURM executor, add an `executor` section like the following to the top-level job definition:

```
{
    ...
    "executor": {
        "name": "slurm",
        "cores": 1,
        "memory": "250MB",
        "walltime": "00:00:10",
        "partition": "debug",
        "processes": 1,
        "local_directory": "/opt/plantit-cluster-home"
    }
    ...
}
```

## Tests

Before running tests, run `bootstrap.sh`, then bring test containers up with `docker-compose -f docker-compose.test.yml up` (`-d` for detached mode).

Unit tests can be run using: `docker-compose -f docker-compose.test.yml exec cluster pytest . -s`

Run integration tests with `docker-compose -f docker-compose.test.yml exec cluster /opt/plantit-cluster/integration-tests.sh`.
