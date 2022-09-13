<div align="center">
<img src="https://github.com/Computational-Plant-Science/plantit/blob/master/plantit/front_end/src/assets/logo.png?raw=true" style="position:relative;top: 75px;width:50px;" />

# plantitCLI

CLI, Python library and science gateway for high-throughput phenotyping on clusters and the CyVerse cloud

- generate job scripts and launch containers to a SLURM cluster with a single command
- add the [`plantit-action`](https://github.com/Computational-Plant-Science/plantit-action) to a GitHub Actions workflow for continuous analysis
- automatically transfer data and results to and from the CyVerse Data Store
- discover or publish workflows and monitor your submissions in the [web UI](https://plantit.cyverse.org/)

![CI](https://github.com/Computational-Plant-Science/plantit-cli/workflows/CI/badge.svg)
[![Coverage Status](https://coveralls.io/repos/github/Computational-Plant-Science/plantit-cli/badge.svg?branch=master)](https://coveralls.io/github/Computational-Plant-Science/plantit-cli) 
![Status](https://img.shields.io/badge/-under%20development-yellow?style=flat-square)

**In development, not stable.**

</div>

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Contents**

- [Requirements](#requirements)
- [Installation](#installation)
- [Quickstart](#quickstart)
- [Usage](#usage)
  - [Commands](#commands)
    - [Token](#token)
    - [User](#user)
    - [List](#list)
    - [Stat](#stat)
    - [Pull](#pull)
    - [Push](#push)
    - [Exists](#exists)
    - [Create](#create)
    - [Share](#share)
    - [Unshare](#unshare)
    - [Tag](#tag)
    - [Tags](#tags)
    - [Scripts](#scripts)
    - [Submit](#submit)
- [Development](#development)
  - [Tests](#tests)
    - [Markers](#markers)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Requirements

- Python 3.8+

## Installation

To install with pip:

```shell
pip install plantit
```

## Quickstart

The `plantit` CLI parses YAML configuration files, generates job scripts, and submits [Singularity](https://docs.sylabs.io/guides/3.10/user-guide/) container workflows to a SLURM scheduler in the local environment or to a remote cluster via SSH.

At minimum, a configuration file must contain:

- `image`
- `entrypoint`
- `workdir`
- `email`
- `queue`

For instance, in `hello.yaml`:

```yaml
image: alpine
entrypoint: echo "hello world"
workdir: /path/to/your/scratch/directory
email: you@institution.edu
queue: batch
```

To generate a job script and submit this job to a scheduler running on the host machine:

```shell
plantit hello.yaml
```

If the job was submitted successfully, the job ID will be printed.

## Usage

To show CLI docs run `plantit -h`. Besides the main `plantit <config file>.yml` command, a number of subcommands can be invoked with `plantit <command>`.

### Commands

The following commands are available:

- `token`: Retrieve a CyVerse authentication token.
- `compat`: Check if the current system is compatible.
- `user`: Retrieve the user's profile information.
- `list`: List files in a collection.
- `stat`: Get information about a file or collection.
- `pull`: Download one or more files from a collection.
- `push`: Upload one or more files to a collection.
- `exists`: Check if a path exists in the data store.
- `create`: Create a collection in the data store.
- `share`: Share a file or collection with another user.
- `unshare`: Revoke another user's access to your file or collection.
- `tag`: Set metadata for a given file or collection.
- `tags`: Get metadata for a given file or collection.
- `scripts`: Generate job scripts for a container workflow.
- `submit`: Submit jobs for a container workflow to a cluster.

#### Token

To request a CyVerse CAS authentication token, use the `token` command:

```shell script
plantit token --username <your CyVerse username> --password <your CyVerse password>
```

The token can be passed via `--token (-t)` argument to authenticate subsequent commands.

#### Compat

The `plantit compat` command determines whether jobs can be submitted to the host system, affirming to `stdout` if the following conditions are met:

- `singularity` is installed and available on the path
- the CyVerse data store is reachable via iRODS or science APIs
  * for the former, the user must have run `iinit` to configure iCommands
- SLURM is up and standard commands `sbatch`, `squeue`, `sacct`, etc are available

Otherwise the command terminates with an error signal and information on the missing or misconfigured dependencies is printed to `stderr`.

#### User

The `user` command can be used to retrieve public profile information for CyVerse users. For instance, to get my profile info:

```shell
plantit user -t <token> wbonelli
```

#### List

To list the contents of a collection in the data store, use the `list` command. For instance:

```shell
plantit list -t <token> /iplant/home/shared/iplantcollaborative/testing_tools/
```

#### Stat

To view metadata for a particular collection or object in the data store, use the `stat` command. For instance:

```shell
plantit stat -t <token> /iplant/home/shared/iplantcollaborative/testing_tools/
```

#### Pull

To download a single file from the data store to the current working directory, simply provide its full path:

```shell
plantit pull -t <token> /iplant/home/shared/iplantcollaborative/testing_tools/cowsay/cowsay.txt
```

To download all files from the `/iplant/home/shared/iplantcollaborative/testing_tools/cowsay/` collection to the current working directory, just provide the collection path instead:

```shell
plantit pull -t <token> /iplant/home/shared/iplantcollaborative/testing_tools/cowsay/
```

Optional arguments are:

- `--local_path (-p)`: Local path to download files to
- `--include_pattern (-ip)`: File patterns to include (0+)
- `--force (-f)`: Whether to overwrite already-existing files

#### Push

To upload all files in the current working directory to the `/iplant/home/<my>/<directory/` in the CyVerse Data Store, use:

```shell
plantit push -t <token> /iplant/home/<username>/<collection>/
```

Optional arguments include:

- `--local_path (-p)`: Local path to download files to
- `--include_pattern (-ip)`: File patterns to include (0+)
- `--include_name (-in)`: File names to include (0+)
- `--exclude_pattern (-ep)`: File patterns to exclude (0+)
- `--exclude_name (-en)`: File names to exclude (0+)

To upload a single file to the data store, provide the `--local_path (-p)` argument. For instance:

```shell script
plantit push -t <token> /iplant/home/<username>/<collection/ -p /my/local/file.txt
```

If only `include_...`s are provided, only the file patterns and names specified will be included. If only `exclude_...`s section are present, all files except the patterns and names specified will be included. If you provide both `include_...` and `exclude_...` sections, the `include_...` rules will first be applied to generate a subset of files, which will then be filtered by the `exclude_...` rules.

#### Exists

To determine whether a particular path exists in the data store, use the `exists` command. For instance, to check if a collection exists:

```shell
plantit exists -t <token> /iplant/home/<username>/<collection
```

The `--type` option can be provided with value `dir` or `file` to verify that the given path is of the specified type.

#### Create

To create a new collection, use the `create` command:

```shell
plantit create -t <token> /iplant/home/<username>/<new collection name>
```

#### Share

To share a file or collection with another user, use the `share` command:

```shell
plantit share -t <token> /iplant/home/<username>/<collection> --username <user to share with> --permission <'read' or 'write'>
```

Note that you must provide both the `--username` and `--permission` flags.

#### Unshare

To revoke another user's access to your file or collection, use the `unshare` command:

```shell
plantit unshare -t <token> /iplant/home/<username>/<collection> --username <username>
```

This applies to both `read` and `write` permissions for the specified user.

#### Tag

To set metadata for a given file object or collection in your data store, use the `tag` command:

```shell
plantit tag <data object ID> -t <token> -a k1=v1 -a k2=v2
```

This applies the two given attributes to the data object (attributes must be formatted `key=value`).

**Warning:** this command is an overwrite, not an append. We do not support appending tags as there is no Terrain endpoint to add/remove individual metadata attributes. Note also that by default, key/value pairs are passed on the `avus` attribute of the request body rather than `irods-avus`, e.g.:

```shell
POST https://de.cyverse.org/terrain/secured/filesystem/<ID>/metadata
{
    "irods-avus": [],
    "avus": [
        {
            "attr": "some key"
            "value": "some value",
            "unit": ""
        }
    ]
}
```

To configure `irods-avus` attributes as well as or in place of standard attributes, use the `--irods_attribute (-ia)` option. Both standard and iRODS attributes can be used in the same invocation.

#### Tags

To retrieve the metadata describing a particular file object or collection, use the `tags` command:

```shell
plantit tags <data object ID> -t <token>
```

This will retrieve standard attributes by default. To retrieve iRODS attributes instead, use the `--irods (-i)` option.

#### Scripts

To generate SLURM job scripts for a container workflow, use the `scripts` command. For instance:

```shell
plantit scripts ...
```

#### Submit

To submit a container workflow as a job script on a cluster, use the `submit` command. For instance, to copy the contents of the current working directory to a cluster and submit the job defined in `job.sh`:

```shell
plantit submit -p . -j job.sh \
  --cluster_host <hostname or IP> \
  --cluster_user <user account name> \
  --cluster_key <private SSH key> \
  --cluster_target <location to copy job scripts and input files>
```

## Development

First, clone the repo with `git clone https://github.com/Computational-Plant-Science/plantit-cli.git`.

Create a Python3 virtual environment, e.g. `python3 -m venv venv`, then install `plantit` and core dependencies with `pip install .`. Install testing and linting dependencies as well with `pip install ".[test]".

### Tests

The tests can be run from the project root with `pytest` (or `python3 -m pytest`). Use `-v` for verbose mode and `-n auto` to run them in parallel on as many cores as your machine will spare.

**Note:** some tests required the `CYVERSE_USERNAME` and `CYVERSE_PASSWORD` environment variables. You can set these manually or put them in a `.env` file in the project root &mdash; `pytest-dotenv` will detect them in the latter case. Test cases will use this CyVerse account and its associated data store as a test environment. Each test case isolates its workspace to a folder named by GUID.

#### Markers

The full test suite should take 5-10 minutes to run, depending on the delay configured to allow the CyVerse Data Store to become consistent. This is 10 seconds per write operation, by default. 

**Note:** The CyVerse data store is not immediately consistent and write operations may take some time to be reflected in subsequent reads. Tests must wait some unknown amount of time to allow the Data Store to update its internal state. If tests begin to fail intermittently, the `DEFAULT_SLEEP` variable in `plantit/terrain/tests/conftest.py` may need to be increased.

A fast subset of the tests can be run with `pytest -S` (short for `--smoke`). The smoke tests should complete in under a minute.