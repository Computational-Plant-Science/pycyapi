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

To run `hello_world.yaml`, use `plantit hello_world.yaml`. The YAML schema should look something like this:

```yaml
identifier: a42033c3-9ba1-4d99-83ed-3fdce08c706e # required
image: docker://alpine                           # required
workdir: /your/working/directory                 # required
command: echo $MESSAGE                           # required
params:                                          # optional
- key: message
  value: Hello, plant person!
```

Taking the elements one at a time:

- `identifier`: the workflow run identifier (e.g., a GUID)
- `image`: the Docker or Singularity container image
- `workdir`: where to execute the workflow
- `command`: the command(s) to run inside the container
- `params`: parameters substituted when `command` runs

### Input/Output

The CLI can automatically copy files from the CyVerse Data Store to the local (or network) file system, then push output back to the Data Store after your run. To direct the CLI to pull a file or directory, add an `input` section (the file or directory name will be substituted for `$INPUT` when the run's `command` is invoked).

Runs involving file IO fall into 3 categories:

- pull a file from the Data Store and spawn a single container to process it
- pull a directory from the Data Store and spawn a single container to process it
- pull a directory from the Data Store and spawn multiple containers to process files in parallel

#### 1 File, 1 Container

To pull a file from the Data Store and spawn a single container to process it, use `kind: file` and `from: <file path>`:

```yaml
input:
  kind: file
  from: /iplant/home/username/directory/file
```

#### 1 Directory, 1 Container

To pull the contents of a directory from the Data Store and spawn a single container to process it, use `kind: directory` and `from: <directory path>`:

```yaml
input:
  kind: directory
  from: /iplant/home/username/directory
```

#### 1 Directory, 1+ Container(s)

To pull a directory from the Data Store and spawn a container for each file, use `kind: file` and `from: <directory path>`:

```yaml
input:
  kind: file
  from: /iplant/home/username/directory
```

To push files matching a pattern to the Data Store after container execution (the local path will be substituted for `$OUTPUT` when the workflow's `command` is executed):

```yaml
output:
  pattern: xslx
  from: directory # relative to the workflow's working directory
  to: /iplant/home/username/collection
```

### Authenticating against the Terrain API

Runs specifying inputs and outputs must provide the `--cyverse_token` flag, since the CLI uses the Terrain API to query and access data in the CyVerse Data Store. For instance, to run `some_definition.yaml`:

```shell script
plantit some_definition.yaml --cyverse_token 'eyJhbGciOiJSUzI1N...'
```

A CyVerse access token can be obtained with a `GET` request to the Terrain API (providing your CyVerse username/password for basic auth):

```shell script
GET https://de.cyverse.org/terrain/token/cas
```

## Examples

Some sample definition files can be found in `examples/`.

## Tests

Before running tests...
 
 1) create an environment file `.env` in the project root, e.g.:
 
```
CYVERSE_USERNAME=your_cyverse_username
CYVERSE_PASSWORD=your_cyverse_password
MYSQL_DATABASE=slurm_acct_db
MYSQL_USER=slurm
MYSQL_PASSWORD=password
```
 
 2) run `scripts/bootstrap.sh` (this will pull/build images for a small `docker-compose` SLURM cluster test environment), then run:

```docker-compose -f docker-compose.test.yml exec slurmctld python3 -m pytest /opt/plantit-cli -s```

Tests invoking the Terrain API may take some time to run; they're rigged with a delay to allow writes to propagate from Terrain to the CyVerse Data Store (some pass/fail non-determinism occurs otherwise).
