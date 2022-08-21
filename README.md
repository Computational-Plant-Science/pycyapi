<div align="center">
<img src="https://github.com/Computational-Plant-Science/plantit/blob/master/plantit/front_end/src/assets/logo.png?raw=true" style="position:relative;top: 75px;width:50px;" />
<h1 class="text-dark">
    <span style="text-decoration: underline">plant<small
                                    class="mb-3 text-success"
                                    style="
                                        text-decoration: underline;
                                        text-shadow: 1px 1px 2px black;
                                        z-index: 100;
                                    "
                                    ><small>it</small></small
                                ><small>CLI</small></span>
</h1>

A CLI and Python client for [plantit](https://plantit.cyverse.org/apis/v1/swagger/).

![CI](https://github.com/Computational-Plant-Science/plantit-cli/workflows/CI/badge.svg)
[![PyPI version](https://badge.fury.io/py/plantit-cli.svg)](https://badge.fury.io/py/plantit-cli)
[![Coverage Status](https://coveralls.io/repos/github/Computational-Plant-Science/plantit-cli/badge.svg?branch=master)](https://coveralls.io/github/Computational-Plant-Science/plantit-cli) 

</div>

**This repository is under active development and is not yet stable.**

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Contents**

- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
  - [Commands](#commands)
    - [Token](#token)
    - [User](#user)
    - [List](#list)
    - [Stat](#stat)
    - [Download](#download)
    - [Upload](#upload)
    - [Exists](#exists)
    - [Create](#create)
    - [Share](#share)
    - [Unshare](#unshare)
    - [Tag](#tag)
    - [Tags](#tags)
- [Development](#development)
  - [Tests](#tests)
    - [Unit tests](#unit-tests)
    - [Integration tests](#integration-tests)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Requirements

- Python 3.6.9+

## Installation

To install with  pip:

```
pip install plantit
```

## Usage

Once installed it can be invoked with `plantit <command>`.

### Commands

The following commands are currently supported:

- `token`: Retrieve an authentication token.
- `user`: Retrieve the user's profile information.
- `list`: List files in a collection.
- `stat`: Get information about a file or collection.
- `download`: Download one or more files from a collection.
- `upload`: Upload one or more files to a collection.
- `exists`: Check if a path exists in the data store.
- `create`: Create a collection.
- `share`: Share a file or collection with another user.
- `unshare`: Revoke another user's access to your file or collection.
- `tag`: Set metadata for a given file or collection.
- `tags`: Get metadata for a given file or collection.

#### Token

To request a CAS authentication token, use the `token` command:

```shell script
pycy token --username <your CyVerse username> --password <your CyVerse password>
```

The token can then be passed in the `--token (-t)` parameter to authenticate further commands.

#### User

The `user` command can be used to retrieve public profile information for CyVerse users. For instance, to get my profile info:

```shell
pycy user -t <token> wbonelli
```

#### List

To list the contents of a collection in the data store, use the `list` command. For instance:

```shell
pycy list -t <token> /iplant/home/shared/iplantcollaborative/testing_tools/
```

#### Stat

To view metadata for a particular collection or object in the data store, use the `stat` command. For instance:

```shell
pycy stat -t <token> /iplant/home/shared/iplantcollaborative/testing_tools/
```

#### Download

To download a single file from the data store to the current working directory, simply provide its full path:

```shell
pycy download -t <token> /iplant/home/shared/iplantcollaborative/testing_tools/cowsay/cowsay.txt
```

To download all files from the `/iplant/home/shared/iplantcollaborative/testing_tools/cowsay/` collection to the current working directory, just provide the collection path instead:

```shell
pycy download -t <token> /iplant/home/shared/iplantcollaborative/testing_tools/cowsay/
```

Optional arguments are:

- `--local_path (-p)`: Local path to download files to
- `--include_pattern (-ip)`: File patterns to include (0+)
- `--force (-f)`: Whether to overwrite already-existing files

#### Upload

To upload all files in the current working directory to the `/iplant/home/<my>/<directory/` in the CyVerse Data Store, use:

```shell script
pycy upload -t <token> /iplant/home/<username>/<collection>/
```

Optional arguments include:

- `--local_path (-p)`: Local path to download files to
- `--include_pattern (-ip)`: File patterns to include (0+)
- `--include_name (-in)`: File names to include (0+)
- `--exclude_pattern (-ep)`: File patterns to exclude (0+)
- `--exclude_name (-en)`: File names to exclude (0+)

To upload a single file to the data store, provide the `--local_path (-p)` argument. For instance:

```shell script
pycy upload -t <token> /iplant/home/<username>/<collection/ -p /my/local/file.txt
```

If only `include_...`s are provided, only the file patterns and names specified will be included. If only `exclude_...`s section are present, all files except the patterns and names specified will be included. If you provide both `include_...` and `exclude_...` sections, the `include_...` rules will first be applied to generate a subset of files, which will then be filtered by the `exclude_...` rules.

#### Exists

To determine whether a particular path exists in the data store, use the `exists` command. For instance, to check if a collection exists:

```shell script
pycy exists -t <token> /iplant/home/<username>/<collection
```

The `--type` option can be provided with value `dir` or `file` to verify that the given path is of the specified type.

#### Create

To create a new collection, use the `create` command:

```shell script
pycy create -t <token> /iplant/home/<username>/<new collection name>
```

#### Share

To share a file or collection with another user, use the `share` command:

```shell script
pycy share -t <token> /iplant/home/<username>/<collection> --username <user to share with> --permission <'read' or 'write'>
```

Note that you must provide both the `--username` and `--permission` flags.

#### Unshare

To revoke another user's access to your file or collection, use the `unshare` command:

```shell script
pycy unshare -t <token> /iplant/home/<username>/<collection> --username <username>
```

This applies to both `read` and `write` permissions for the specified user.

#### Tag

To set metadata for a given file object or collection in your data store, use the `tag` command:

```shell script
pycy tag <data object ID> -t <token> -a k1=v1 -a k2=v2
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

```shell script
pycy tags <data object ID> -t <token>
```

This will retrieve standard attributes by default. To retrieve iRODS attributes instead, use the `--irods (-i)` option.

## Development

To set up a development environment, clone the repo with `git clone https://github.com/Computational-Plant-Science/plantit-cli.git`. You can create a Python3 virtual environment with e.g., `python3 -m venv .`, then install dependencies with `pip install -r requirements.txt`.

### Tests

The full test suite can be run from the project root with `pytest` (or `python3 -m pytest`).

**Note:** to run integration tests, you must set the `CYVERSE_USERNAME` and `CYVERSE_PASSWORD` environment variables.

#### Unit tests

To run unit tests, invoke `pytest` from the project root:

```shell script
pytest plantit/tests/unit
```

#### Integration tests

Integration tests can be run from the project root with:

```shell script
pytest plantit/tests/integration
```

As mentioned above, you must set the `CYVERSE_USERNAME` and `CYVERSE_PASSWORD` environment variables before running integration tests. The test cases will use this CyVerse account and its associated data store as a test environment.

Note also that the CyVerse data store is not immediately consistent and write operations may take some time to become visible to reads, thus integration tests must impose an artificial delay. If tests begin to fail intermittently, the value of the `DEFAULT_SLEEP` constant in `pycyapi/tests/integration/utils.py` may need to be increased.