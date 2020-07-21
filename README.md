# PlantIT CLI [![Build Status](https://travis-ci.com/Computational-Plant-Science/plantit-cli.svg?branch=master)](https://travis-ci.com/Computational-Plant-Science/plantit-cli)

PlantIT workflow management CLI. Deploy PlantIT workflows on laptops, servers, or HPC/HTC clusters. 

**This project is in open alpha and is not yet stable.**

## Requirements


- Python 3.6.9+
- [Singularity](https://sylabs.io/docs/)

## Installation

To install, clone the project with `git clone https://github.com/Computational-Plant-Science/plantit-cli.git` or use pip:

```
pip3 install plantit-cli
```

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

### Executor

The `executor` option specifies how to run the workflow on underlying computing resources. Currently `in-process`, `pbs`, and `slurm`  executors are supported. If no executor is specified in the job definition file, the CLI will default to the `in-process` executor.

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

### Input/Output

The `plantit-cli` can automatically copy input files from iRODS onto the local (or network) file system, then push output files back to iRODS after your workflow runs. To direct `plantit-cli` to pull an input file or directory from iRODS, add an `input` section (the file or directory name will be substituted for `$INPUT` when the workflow's `command` is executed).

To configure a workflow to pull a single file from iRODS and spawn a single container to process it, use `kind: file` and a file `path`:

```yaml
input:
  kind: file
  path: /iplant/home/username/directory/file
```

To configure a workflow to pull the contents of a directory from iRODS and spawn a single container to process it, use `kind: directory` and a directory `path`:

```yaml
input:
  kind: directory
  path: /iplant/home/username/directory
```

To configure a workflow to pull a directory from iRODS and spawn multiple containers to process files in parallel, use `kind: file` and a directory `path`:

```yaml
input:
  kind: file
  path: /iplant/home/username/directory
```

To push a local file or the contents of a local directory to iRODS (the local path will be substituted for `$OUTPUT` when the workflow's `command` is executed):

```yaml
output:
  local_path: directory # relative to the workflow's working directory
  irods_path: /iplant/home/username/collection
```

#### Default iRODS connection configuration

If `plantit-cli` detects a `~/.irods/irods_environment.json` file, it will by default connect to the iRODS instance specified therein.

#### Overriding the default iRODS connection configuration

To override `plantit-cli`'s default iRODS connection configuration, use the following named arguments:

- `--irods_host`: the iRODS host (FQDN or IP address)
- `--irods_port`: the iRODS port (usually 1247)
- `--irods_username` the iRODS username
- `--irods_password` the iRODS password

Note that `plantit-cli` expects all of these together or none of them.

## Tests

Before running tests, run `scripts/bootstrap.sh`.

To run unit tests:

```docker-compose -f docker-compose.test.yml run cluster /bin/bash /root/wait-for-it.sh irods:1247 -- pytest . -s```

To run integration tests:

```docker-compose -f docker-compose.test.yml run cluster /bin/bash /root/wait-for-it.sh irods:1247 -- chmod +x scripts/test-cli.sh && ./scripts/test-cli.sh```
