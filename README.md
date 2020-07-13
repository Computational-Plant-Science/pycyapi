# PlantIT CLI [![Build Status](https://travis-ci.com/Computational-Plant-Science/plantit-cli.svg?branch=master)](https://travis-ci.com/Computational-Plant-Science/plantit-cli)

PlantIT workflow management CLI. Deploy PlantIT workflows on laptops, servers, or HPC/HTC clusters. 

**This project is in alpha and is not yet stable.**

## Requirements

- Python 3.6.9+
- [Singularity](https://sylabs.io/docs/)

## Installation

To install, clone the project with `git clone https://github.com/Computational-Plant-Science/plantit-cli.git` or use pip:

```
pip3 install plantit-cli
```

### Python Dependencies

- [Dagster](https://docs.dagster.io/)
- [Dask](https://dask.org/)
- [Dask-Jobqueue](https://jobqueue.dask.org/en/latest/)

## Usage

To run a workflow defined in `workflow.yaml`, use `plantit workflow.yaml --token <PlantIT API authentication token>`. The YAML schema should look something like this:

```yaml
identifier: a42033c3-9ba1-4d99-83ed-3fdce08c706e
image: docker://alpine
workdir: /your/working/directory
command: echo $MESSAGE
params:
- key: message
  value: Hello, plant person!
executor:
  in-process:
api_url: http://plantit/apis/v1/runs/a42033c3-9ba1-4d99-83ed-3fdce08c706e/update_target_status/
```

Taking the elements one at a time:

- `identifier`: the workflow run identifier (GUID)
- `image`: the Docker or Singularity container image
- `workdir`: where to execute the workflow
- `command`: the command(s) to run inside the container
- `params`: parameters substituted when `command` runs
- `executor`: how to execute the pipeline (e.g., in-process or on an HPC/HTC resource manager)
- `api_url`: the PlantIT API endpoint to relay run status updates

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

### Inputs and Outputs

Currently iRODS is the only supported data source. Amazon S3/Google Cloud Storage integrations are planned.

To direct `plantit-cli` to pull an input file or directory from a remote data source, add an `input` section. The file or directory path will be substituted for `$INPUT` when the workflow's `command` is executed.

To configure a single container to operate on a single file, use `file` for attribute `kind`. For example, to pull the file from an iRODS data store:

```yaml
input:
  kind: file
  host: data.cyverse.org
  irods_path: /iplant/home/your_username/your_collection/your_file
  password: your_password
  port: '1247'
  username: your_username
  zone: iplant
```

To configure a single container to operate on a directory, use `kind: directory` and a directory path for `irods_path`. For example:

```yaml
input:
  kind: directory
  host: data.cyverse.org
  irods_path: /iplant/home/your_username/your_directory
  password: your_password
  port: '1247'
  username: your_username
  zone: iplant
```

To configure multiple containers to operate in parallel on the files in a directory, use `kind: file` and a directory path for `irods_path`. For example:

```yaml
input:
  kind: file
  host: data.cyverse.org
  irods_path: /iplant/home/your_username/your_directory
  password: your_password
  port: '1247'
  username: your_username
  zone: iplant
```

To push a local file or directory to an iRODS data store (the local path will be substituted for `$OUTPUT` when the workflow's `command` is executed):

```yaml
output:
  host: data.cyverse.org
  # local_path: /your/working/directory/your_output_file
  local_path: /your/working/directory/your_output_directory/
  irods_path: /iplant/home/your_username/your_collection
  password: your_password
  port: '1247'
  username: your_username
  zone: iplant
```

## Tests

Before running tests, run `scripts/bootstrap.sh`.

Unit tests can then be run with: `docker-compose -f docker-compose.test.yml run cluster /bin/bash /root/wait-for-it.sh irods:1247 -- pytest . -s`
