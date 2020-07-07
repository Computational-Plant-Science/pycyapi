# PlantIT Cluster [![Build Status](https://travis-ci.com/Computational-Plant-Science/plantit-cluster.svg?branch=master)](https://travis-ci.com/Computational-Plant-Science/plantit-cluster)

Executes jobs on local or distributed compute resources.

**This project is under active development and is not yet stable.**

## Requirements

The following are required to run `plantit-cluster` in a Unix environment:

- Python 3.6.9+
- [Singularity](https://sylabs.io/docs/)

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

To execute a job defined in `example_pipeline.json`, run `plantit run example_pipeline.json`. The JSON definition should conform to the following schema:

```json
{
    "workdir": "/test",
    "image": "docker://alpine:latest",
    "commands": [
        "echo",
        "$MESSAGE",
        "&&",
        "cat",
        "$INPUT"
    ],
    "params":[
        "MESSAGE=Hello!"
    ],
    "input": {
        "host":"irods",
        "port": 1247,
        "user":"rods",
        "password":"rods",
        "zone":"testZone",
        "path": "testCollection",
        "param": "file"
    },
    "executor": {
        "name": "in-process"
    }
}
```

Currently `local`, `pbs`, and `slurm`  executors are supported. If no executor is specified in the job definition file, `plantit-cluster` will default to the `local` (in-process) executor.

To use the PBS executor, add an `executor` section like the following to the top-level job definition:

```
{
    ...
    "executor": {
        "name": "pbs",
        "cores": 1,
        "memory": "250MB",
        "walltime": "00:00:10",
        "processes": 1,
        "local_directory": "/your/scratch/directory",
        "n_workers": 1
    }
    ...
}
```

To use the SLURM executor:

To use the PBS executor, add an `executor` section like the following to the top-level job definition:

```
{
    ...
    "executor": {
        "name": "slurm",
        "cores": 1,
        "memory": "250MB",
        "walltime": "00:00:10",
        "processes": 1,
        "local_directory": "/your/scratch/directory",
        "n_workers": 1,
        "partition": "debug",
    }
    ...
}
```

## Tests

Before running tests, run `scripts/bootstrap.sh`.

Unit tests can then be run with: `docker-compose -f docker-compose.test.yml run cluster /bin/bash /root/wait-for-it.sh irods:1247 -- pytest . -s`
