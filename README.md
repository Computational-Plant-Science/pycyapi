<div align="center">

<img src="https://github.com/Computational-Plant-Science/plantit/blob/master/plantit/front_end/src/assets/logo.png?raw=true" style="position:relative;top: 75px" />

<h1 align="center"
    style="text-decoration: underline;">
        plant<small
            class="mb-3 text-success"
            style="text-decoration: underline;text-shadow: 1px 0 0 #000, 0 -1px 0 #000, 0 1px 0 #000, -1px 0 0 #000; color: #d6df5D"
            >IT</small><small style="text-decoration: underline;text-shadow: 1px 0 0 #000, 0 -1px 0 #000, 0 1px 0 #000, -1px 0 0 #000; color: #dee2e6">CLI</small>
</h1>

Container orchestration for reproducible phenotyping workflows on laptops, servers, or HPC

![CI](https://github.com/Computational-Plant-Science/plantit-cli/workflows/CI/badge.svg)
[![PyPI version](https://badge.fury.io/py/plantit-cli.svg)](https://badge.fury.io/py/plantit-cli)
[![Coverage Status](https://coveralls.io/repos/github/Computational-Plant-Science/plantit-cli/badge.svg?branch=master)](https://coveralls.io/github/Computational-Plant-Science/plantit-cli) 

</div>

Features:

- Parallel transfers to/from the CyVerse Data Store with Terrain API
- Deploy Docker images as Singularity containers on clusters/servers
- Compatible with any cluster scheduler supported by [Dask-Jobqueue](https://jobqueue.dask.org/en/latest/index.html)

This package must be installed and available in the $PATH on agents bound to PlantIT.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Contents**

- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
  - [Commands](#commands)
    - [Pull](#pull)
    - [Run](#run)
      - [Compound commands](#compound-commands)
      - [Inputs](#inputs)
        - [File](#file)
        - [Files](#files)
        - [Directory](#directory)
      - [Bind mounts](#bind-mounts)
      - [CUDA GPU mode](#cuda-gpu-mode)
      - [HPC/HTC](#hpchtc)
        - [Virtual memory](#virtual-memory)
        - [Other resource requests](#other-resource-requests)
    - [Clean](#clean)
    - [Zip](#zip)
    - [Push](#push)
    - [Ping](#ping)
  - [Authenticating with Docker](#authenticating-with-docker)
  - [Authenticating with Terrain](#authenticating-with-terrain)
  - [Authenticating with PlantIT](#authenticating-with-plantit)
  - [Logging](#logging)
- [Development](#development)
  - [Tests](#tests)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Requirements


- Python 3.6.9+
- [Singularity](https://sylabs.io/docs/)

## Installation

To install the PlantIT CLI, use pip:

```
pip3 install plantit-cli
```

## Usage

Once the CLI is installed it can be invoked with `plantit <command>`.

### Commands

The CLI supports the following commands:

- `ping`: Print `pong`.
- `pull`: Download files from the CyVerse Data Store.
- `run`: Run a workflow.
- `clean`: Remove patterns from result files.
- `zip`: Zip files produced by a workflow.
- `push`: Upload files to the CyVerse Data Store.

#### Pull

To pull files from the `/iplant/home/shared/iplantcollaborative/testing_tools/cowsay/` directory in the CyVerse Data Store to the current working directory, use:

```shell
plantit terrain pull /iplant/home/shared/iplantcollaborative/testing_tools/cowsay/ --terrain_token <token>
```

Optional arguments are:

- `--local_path (-p)`: Local path to download files to.
- `--pattern`: File patterns to include (one or more).
- `--overwrite`: Whether to overwrite already-existing files.

#### Run

PlantIT workflows are defined in YAML files. To run a workflow defined in `hello_world.yaml`, use `plantit run hello_world.yaml`. At minimum, the schema should include the following attributes:

```yaml
image: docker://alpine              # Docker image
workdir: /your/working/directory    # working directory
command: echo "Hello, world!"       # entrypoint
```

##### Compound commands

Note that your `command` may fail on some images if it contains `&&`. If you must run multiple consecutive commands, it's probably best to package them into a script.

##### Inputs

Runs involving inputs fall into 3 categories:

- spawn a single container to process a single file
- spawn a single container to process a single directory
- spawn a container per file to process files in a directory

To pull a file or directory, add an `input` section.

###### File

To pull a file from the Data Store and spawn a single container to process it, use `kind: file` and `from: <file path>`:

```yaml
input:
  kind: file
  path: /iplant/home/username/directory/file
```

###### Files

To pull a directory from the Data Store and spawn a container for each file, use `kind: files` and `from: <directory path>`:

```yaml
input:
  kind: files
  path: /iplant/home/username/directory
  patterns:  # optional
  - jpg
  - png
```

###### Directory

To pull the contents of a directory from the Data Store and spawn a single container to process it, use `kind: directory` and `from: <directory path>`:

```yaml
input:
  kind: directory
  path: /iplant/home/username/directory
```

##### Bind mounts

If your code needs to write temporary files somewhere other than the (automatically mounted) host working directory, use the `bind_mounts` attribute:

```yaml
bind_mounts:
  - /path/in/your/container # defaults to the host working directory
  - path/relative/to/host/working/directory:/another/path/in/your/container
```

##### CUDA GPU mode

To instruct Singularity to bind to NVIDIA GPU drivers on the host, add a `gpu: True` attribute to your configuration.

##### HPC/HTC

On high-performance or high-throughput computing systems with a scheduler like Torque or SLURM, you can parallelize multi-file runs by adding a `jobqueue` section like the following:

```yaml
...
jobqueue:
  slurm:
    cores: 1
    processes: 10,
    project: '<your allocation>'
    walltime: '01:00:00'
    queue: '<your queue>'
```

Substitute `pbs`, `moab`, `slurm`, or any other [Dask Jobqueue](https://jobqueue.dask.org/) cluster configuration section (the CLI uses Dask internally and passes your configuration directly through).

###### Virtual memory

For clusters with virtual memory, you may need to use `header_skip` to alter Dask's resource request from the scheduler:

```yaml
...
jobqueue:
  slurm:
    ...
    header_skip:
      - '--mem' # for clusters with virtual memory
```

###### Other resource requests

You can add other cluster-specific resource requests, like GPU-enabled nodes, with an `extra` section:

```yaml
...
jobqueue:
  slurm:
    ...
    extra:
      - '--gres=gpu:1'
```

#### Clean

Due to cluster scheduler configuration quirks, when invoking the CLI with Docker username/password these secrets may end up in job output files. The `clean` command can be used to remove arbitrary patterns from files &mdash; it's recommended out of an abundance of caution to *always* use it prior to the `zip` or `push` commands.

To remove `secret_password`  from `myjob.1234567.out` and `myjob.1234567.err` run:

```bash
plantit clean myjob.1234567.out myjob.1234567.err -p 'secret_password'
```

#### Zip

To zip files all files in a directory, use `plantit zip <input directory>`.

To include file patterns or names, use (one or more) flags `--include_pattern` (abbr. `-ip`) or `--include_name` (`-in`).

To exclude file patterns or names, use (one or more) flags `--exclude_pattern` (`-ep`) or `--exclude_name` (`-en`).

Included files are gathered first, then excludes are filtered out of this collection.

#### Push

To push files in the current working directory to the `/iplant/home/<my>/<directory/` in the CyVerse Data Store, use `plantit terrain push /iplant/home/<my>/<directory/ --terrain_token <token>`.

Options are:

- `--local_path (-p)`: Local path to download files to.
- `--include_pattern (-ip)`: File patterns to include (one or more).
- `--include_name (-in)`: File names to include (one or more).
- `--exclude_pattern (-ep)`: File patterns to exclude (one or more).
- `--exclude_name (-en)`: File names to exclude (one or more).

If only `include_...`s are provided, only the file patterns and names specified will be included. If only `exclude_...`s section are present, all files except the patterns and names specified will be included. If you provide both `include_...` and `exclude_...` sections, the `include_...` rules will first be applied to generate a subset of files, which will then be filtered by the `exclude_...` rules.

<!--#### Verifying input file checksums

To verify checksums associated with input files, add a `checksums` attribute to the `input` section, with a list of `name`/`md5` pairs. For instance:

```yaml
input:
  kind: directory
  from: /iplant/home/username/directory
  checksums: 
    - name: file1.txt
      md5: 94fc3699a0f99317534736f0ec982dea
    - name: file2.txt
      md5: 8540f05638ac10899e8bc31c13d5074a
```-->

#### Ping

The `plantit ping` command is used internally by the PlantIT web application to test whether the CLI is properly installed on user-defined agents.

### Authenticating with Docker

To authenticate with Docker and bypass Docker Hub rate limits, provide a `--docker_username` and `--docker_password`. For instance:

```shell
plantit run hello_world.yaml --docker_username <your username> --docker_password <your password>
```

This is only required for the `plantit run` command.

### Authenticating with Terrain

The `pull`, `push`, and `run` commands use the Terrain API to access the CyVerse Data Store. Runs with inputs and outputs must provide a `--cyverse_token` argument. For instance, to run `hello_world.yaml`:

```shell
plantit run hello_world.yaml --cyverse_token 'eyJhbGciOiJSUzI1N...'
```

A CyVerse access token can be obtained from the Terrain API with a `GET` request (providing username/password for basic auth):

```shell script
GET https://de.cyverse.org/terrain/token/cas
```

### Authenticating with PlantIT

When the `run` command is invoked, `--plantit_url` and `--plantit_token` options may be provided to authenticate with PlantIT's RESTful API and push task status updates and logs back to the web application. This is only intended for internal use &mdash; requests with an invalid token or for a nonexistent task will be rejected.

### Logging

By default, the CLI will print all output to `stdout`. If a `--plantit_url` and `--plantit_token` are provided, output will be POSTed back to the PlantIT web application (only output generated by the CLI itself &mdash; container output will just be printed to `stdout`).  This is suitable for most cluster deployment targets, whose schedulers should automatically capture job output. To configure the CLI itself to write container output to a file, add the following to your configuration file:

```yaml
log_file: relative/path/to/logfile
```

## Development

To set up a development environment, clone the repo with `git clone https://github.com/Computational-Plant-Science/plantit-cli.git`. Then run `scripts/bootstrap.sh` (this will pull/build images for a small `docker-compose` SLURM cluster test environment).

### Tests

To run unit tests:

```docker compose -f docker-compose.test.yml run -w /opt/plantit-cli/runs slurmctld python3 -m pytest /opt/plantit-cli/plantit_cli/tests/unit -s```

Note that integration tests invoke the Terrain API and may take some time to complete; they're rigged with a delay to allow writes to propagate from Terrain to the CyVerse Data Store (some pass/fail non-determinism occurs otherwise). To run integration tests:

```docker compose -f docker-compose.test.yml run -w /opt/plantit-cli/runs slurmctld python3 -m pytest /opt/plantit-cli/plantit_cli/tests/integration -s```
