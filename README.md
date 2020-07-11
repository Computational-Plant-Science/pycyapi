# PlantIT Pipeline [![Build Status](https://travis-ci.com/Computational-Plant-Science/plantit-cluster.svg?branch=master)](https://travis-ci.com/Computational-Plant-Science/plantit-cluster)

PlantIT pipeline management CLI. Deploy to a laptop, server, or HPC/HTC environment.

**This project is under active development and is not yet stable.**

## Requirements

- Python 3.6.9+
- [Singularity](https://sylabs.io/docs/)

### Python Dependencies

- [Dagster](https://docs.dagster.io/)
- [Dask](https://dask.org/)
- [Dask-Jobqueue](https://jobqueue.dask.org/en/latest/)

## Installation

To install, clone the project with `git clone https://github.com/Computational-Plant-Science/plantit-pipeline.git` or use pip:

```
pip3 install plantit-cluster
```

## Usage

To deploy a pipeline defined in `pipeline.yaml`, run `plantit pipeline pipeline.yaml`. The YAML schema should look something like this:

```yaml
workdir: "/your/working/directory"         
image: docker://alpine:latest             # the Docker or Singularity container image
command: echo $MESSAGE && cat $INPUT"     # the command(s) to run inside the container
params:                                   # parameters substituted  when command runs
- MESSAGE=Hello!
executor:                                 # execute the pipeline in a local process
  in-process:

```

To access an iRODS data store, configure the following section(s):

```yaml
source:
  host: irods
  port: 1247
  user: rods
  password: rods
  zone: testZone
  path: "/testZone/testCollection/"
sink:
  host: irods
  port: 1247
  user: rods
  password: rods
  zone: testZone
  path: "/testZone/testCollection/"
```

Currently `in-process`, `pbs`, and `slurm`  executors are supported. If no executor is specified in the job definition file, the CLI will default to the `in-process` executor.

To use the PBS executor, add an `executor` section like the following:

```yaml
executor:
  pbs:
    cores: 1
    memory: 1GB
    walltime: '00:10:00'
    processes: 1
    local_directory: "/your/scratch/directory"
    n_workers: 1
```

To use the SLURM executor:

```yaml
executor:
  slurm:
    cores: 1
    memory: 1GB
    walltime: '00:10:00'
    processes: 1
    local_directory: "/your/scratch/directory"
    n_workers: 1
    partition: debug
```

## Tests

Before running tests, run `scripts/bootstrap.sh`.

Unit tests can then be run with: `docker-compose -f docker-compose.test.yml run cluster /bin/bash /root/wait-for-it.sh irods:1247 -- pytest . -s`
