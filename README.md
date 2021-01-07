<p align="center">
<img src="https://github.com/Computational-Plant-Science/plantit/blob/master/plantit/front_end/src/assets/logo.png?raw=true" />
</p>

# PlantIT CLI

![CI](https://github.com/Computational-Plant-Science/plantit-cli/workflows/CI/badge.svg)
[![PyPI version](https://badge.fury.io/py/plantit-cli.svg)](https://badge.fury.io/py/plantit-cli)
[![Coverage Status](https://coveralls.io/repos/github/Computational-Plant-Science/plantit-cli/badge.svg?branch=master)](https://coveralls.io/github/Computational-Plant-Science/plantit-cli) 

Deploy workflows on laptops, servers, or HPC/HTC clusters.

**This project is in open alpha and is not yet stable.**

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Contents**

- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
  - [Inputs](#inputs)
    - [Input kinds](#input-kinds)
      - [Input file](#input-file)
      - [Input files](#input-files)
      - [Input directory](#input-directory)
    - [Input patterns](#input-patterns)
    - [Overwriting existing input files](#overwriting-existing-input-files)
  - [Outputs](#outputs)
  - [Bind mounts](#bind-mounts)
  - [SLURM parallelism](#slurm-parallelism)
  - [Authenticating with the Terrain API](#authenticating-with-the-terrain-api)
  - [Authenticating with PlantIT](#authenticating-with-plantit)
  - [Logging](#logging)
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

To run `hello_world.yaml`, use `plantit hello_world.yaml`. The YAML schema should look something like this:

```yaml
identifier: a42033c3-9ba1-4d99-83ed-3fdce08c706e # run identifier (required)
image: docker://alpine                           # Docker or Singularity image (required)
workdir: /your/working/directory                 # working directory (required)
command: echo $MESSAGE                           # command to run in container (required)
params:                                          # parameters substituted when `command` is run (optional)
- key: message
  value: Hello, plant person!
```

Note that your `command` may fail on some operating systems if it contains `&&`. If you must run multiple consecutive commands, you should package them into a script.

### Inputs

The CLI can automatically copy files from the CyVerse Data Store to the local (or network) file system before your code runs, then push output files back to the Data Store afterwards.

#### Input kinds

Runs involving inputs and outputs fall into 3 categories:

- pull a file from the Data Store and spawn a single container to process it
- pull a directory from the Data Store and spawn a single container to process it
- pull a directory from the Data Store and spawn multiple containers, one for each file

To pull a file or directory, add an `input` section (the file or directory name will be substituted for `$INPUT` when `command` is invoked).

##### Input file

To pull a file from the Data Store and spawn a single container to process it, use `kind: file` and `from: <file path>`:

```yaml
input:
  kind: file
  from: /iplant/home/username/directory/file
```

##### Input files

To pull a directory from the Data Store and spawn a container for each file, use `kind: files` and `from: <directory path>`:

```yaml
input:
  kind: files
  from: /iplant/home/username/directory
```

##### Input directory

To pull the contents of a directory from the Data Store and spawn a single container to process it, use `kind: directory` and `from: <directory path>`:

```yaml
input:
  kind: directory
  from: /iplant/home/username/directory
```

#### Input patterns

To match and pull only certain input files `from` an input directory, use attribute `patterns`:

```yaml
input:
  kind: directory
  from: /iplant/home/username/directory
  filetypes:
    - jpg
    - png
```

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

#### Overwriting existing input files

Note that by default, the CLI will check whether files already exist on the local filesystem, and will not re-download them if they do. To force a download and overwrite, add the flag `overwrite: True` to the `input` section, for instance:

```yaml
input:
  kind: directory
  from: /iplant/home/username/directory
  overwrite: True
```

### Outputs

To push files back to the Data Store after your container executes, add an `output` section:

```yaml
output:
  from: directory                       # required, relative to the working directory (leave empty to indicate working directory)
  to: /iplant/home/username/collection  # required, path in CyVerse Data Store
```

The path specified in `from` will be substituted for `$OUTPUT` when `command` runs.

By default, all files in `from` are pushed to the location specified in `to` in the CyVerse Data Store (collections will be created as necessary if they do not already exist). To explicitly specify files to include/exclude, add the following to your `output` section:

```yaml
output:
  from: directory                       
  to: /iplant/home/username/collection  
  include:
    patterns:                           # include excel files
      - xlsx
    names:                              # say we want to include a few images,
      - image.png                       # but not all .jpg or .png files...
      - image.jpg
  exclude:
    patterns:
      - temp                            # don't include temp files
    names:
      - not_important.xlsx              # actually, here's an excel file we want to exclude
```

If only an `include` section is provided, only the file patterns and names specified will be included. If only an `exclude` section is present, all files except the patterns and names specified will be included. If you provide both `include` and `exclude` sections, the `include` rules will first be applied to generate a subset of files, which will then be filtered by the `exclude` rules.

### Bind mounts

If your code needs to write temporary files somewhere other than the (automatically mounted) host working directory, use the `mount` attribute:

```yaml
mount:
  - /path/in/your/container # defaults to the host working directory
  - path/relative/to/host/working/directory:/another/path/in/your/container
```

### SLURM deployments

When using `plantit-cli` on SLURM clusters, you can parallelize multi-file runs by adding a section like the following:

```yaml
slurm:
  cores: 1
  processes: 10,
  project: '<allocation>'
  walltime: '01:00:00'
  queue: 'normal'
```

#### Virtual memory

For clusters with virtual memory, you may need to use `header_skip`:

```yaml
slurm:
  ...
  header_skip: # for clusters with virtual memory
    - '--mem'
```

#### Other resource requests

You can add other cluster-specific resource requests, like GPU-enabled nodes, with an `extra` section:

```yaml
slurm:
  ...
  extra:
    - '--gres=gpu:1'
```

### Authenticating with Docker

Docker Hub pull rate limits are quickly reached for large datasets. To authenticate and bypass rate limits, provide a `--docker_username` and `--docker_password`. For instance:

```shell
plantit hello_world.yaml --docker_username <your username> --docker_password <your password>
```

### Authenticating with the Terrain API

The CLI uses the Terrain API to access the CyVerse Data Store. Runs with inputs and outputs must provide a `--cyverse_token` argument. For instance, to run `hello_world.yaml`:

```shell
plantit hello_world.yaml --cyverse_token 'eyJhbGciOiJSUzI1N...'
```

A CyVerse access token can be obtained from the Terrain API with a `GET` request (providing username/password for basic auth):

```shell script
GET https://de.cyverse.org/terrain/token/cas
```

### Authenticating with PlantIT

When the CLI is invoked by PlantIT, a `--plantit_token` is provided in order to authenticate with PlantIT's RESTful API and push run status updates and log messages back to the web application.

### Logging

By default, the CLI will print all output to `stdout`. If a `--plantit_token` is provided, CLI output will be POSTed back to the PlantIT web application (only output generated by the CLI itself; container output will just be printed to `stdout`).

The default configuration is suitable for most cluster deployment targets, whose schedulers should automatically capture job output. To configure the CLI itself to write container output to a file, add the following to your configuration file:

```yaml
logging:
  file: relative/path/to/logfile
```

Note that a `logging` section like the following is equivalent to omitting the section entirely (i.e., the default logging configuration):

```yaml
logging:
  console:
```

## Tests

Before running tests, run `scripts/bootstrap.sh` (this will pull/build images for a small `docker-compose` SLURM cluster test environment). To run unit tests:

```docker-compose -f docker-compose.test.yml run slurmctld python3 -m pytest /opt/plantit-cli/plantit_cli/tests/unit -s```

Note that integration tests invoke the Terrain API and may take some time to complete; they're rigged with a delay to allow writes to propagate from Terrain to the CyVerse Data Store (some pass/fail non-determinism occurs otherwise). To run integration tests:

```docker-compose -f docker-compose.test.yml run slurmctld python3 -m pytest /opt/plantit-cli/plantit_cli/tests/integration -s```
