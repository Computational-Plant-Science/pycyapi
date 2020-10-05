<p align="center">
<img src="https://github.com/Computational-Plant-Science/plantit/blob/master/plantit/front_end/src/assets/logo.png?raw=true" />
</p>

# PlantIT CLI

[![PyPI version](https://badge.fury.io/py/plantit-cli.svg)](https://badge.fury.io/py/plantit-cli) [![Build Status](https://travis-ci.com/Computational-Plant-Science/plantit-cli.svg?branch=master)](https://travis-ci.com/Computational-Plant-Science/plantit-cli) [![Coverage Status](https://coveralls.io/repos/github/Computational-Plant-Science/plantit-cli/badge.svg)](https://coveralls.io/github/Computational-Plant-Science/plantit-cli)

Deploy workflows on laptops, servers, or HPC/HTC clusters.

**This project is in open alpha and is not yet stable.**

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Contents**

- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
  - [Executor](#executor)
  - [Input/Output](#inputoutput)
    - [Authenticating against the Terrain API](#authenticating-against-the-terrain-api)
- [Examples](#examples)
- [Tests](#tests)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

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
identifier: a42033c3-9ba1-4d99-83ed-3fdce08c706e # required
image: docker://alpine # required
workdir: /your/working/directory # required
command: echo $MESSAGE # required
params:
- key: message
  value: Hello, plant person!
executor: # defaults to 'local' if not specified
  local:
api_url: http://plantit/apis/v1/runs/a42033c3-9ba1-4d99-83ed-3fdce08c706e/update_target_status/
```

Taking the elements one at a time:

- `identifier`: the workflow run identifier (GUID)
- `image`: the Docker or Singularity container image
- `workdir`: where to execute the workflow
- `command`: the command(s) to run inside the container
- `params`: parameters substituted when `command` runs
- `executor`: how to execute the pipeline (e.g., locally or on an HPC/HTC resource manager)
- `api_url`: the PlantIT API endpoint to relay run status updates

### Executor

The `executor` option specifies how to run the workflow on underlying computing resources. Currently `local`, `pbs`, and `slurm`  executors are supported. If no executor is specified in the job definition file, the CLI will default to the `local` executor.

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

The `plantit-cli` can automatically copy input files from the CyVerse Data Store onto the local (or network) file system, then push output files back to the Data Store after your workflow runs. To direct `plantit-cli` to pull an input file or directory, add an `input` section (the file or directory name will be substituted for `$INPUT` when the workflow's `command` is executed).

To configure a workflow to pull a single file from the Data Store and spawn a single container to process it, use `kind: file` and `from: <file path>`:

```yaml
input:
  kind: file
  from: /iplant/home/username/directory/file
```

To configure a workflow to pull the contents of a directory from the Data Store and spawn a single container to process it, use `kind: directory` and `from: <directory path>`:

```yaml
input:
  kind: directory
  from: /iplant/home/username/directory
```

To configure a workflow to pull a directory from the Data Store and spawn multiple containers to process files in parallel, use `kind: file` and `from: <directory path>`:

```yaml
input:
  kind: file
  from: /iplant/home/username/directory
```

To configure a flow to push files matching a pattern to the Data Store after container execution (the local path will be substituted for `$OUTPUT` when the workflow's `command` is executed):

```yaml
output:
  pattern: xslx
  from: directory # relative to the workflow's working directory
  to: /iplant/home/username/collection
```

#### Authenticating against the Terrain API

The CLI uses the Terrain API to query and access data in the CyVerse Data Store and expects a `--cyverse_token` flag.

## Examples

Example configuration files can be found in `examples/`.

## Tests

Before running tests, run `scripts/bootstrap.sh`. Then:

```docker-compose -f docker-compose.test.yml run cluster python3 -m pytest . -s```
