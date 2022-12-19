<div align="center">
<img src="de.png" style="position:relative;top: 75px;width:50px;" />

# PyCyAPI

unofficial Python/CLI client for the [CyVerse](https://www.cyverse.org/) [Science](https://cyverse.org/Science-APIs) (a.k.a. [Terrain](https://de.cyverse.org/terrain/docs/index.html)) APIs

![CI](https://github.com/Computational-Plant-Science/pycyapi/workflows/CI/badge.svg)
[![GitHub tag](https://img.shields.io/github/tag/Computational-Plant-Science/pycyapi.svg)](https://github.com/Computational-Plant-Science/pycyapi/tags/latest)
[![PyPI Version](https://img.shields.io/pypi/v/pycyapi.png)](https://pypi.python.org/pypi/pycyapi)
[![PyPI Status](https://img.shields.io/pypi/status/pycyapi.png)](https://pypi.python.org/pypi/pycyapi)
[![PyPI Versions](https://img.shields.io/pypi/pyversions/pycyapi.png)](https://pypi.python.org/pypi/pycyapi)

</div>

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [Status](#status)
- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
  - [Authenticating](#authenticating)
  - [Commands](#commands)
    - [Version](#version)
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

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Status

API coverage is still sparse, focusing mainly on `data` and `fileio` endpoints. It will likely stay this way as this project exists predominantly for consumption by [`plantit`](https://github.com/Computational-Plant-Science/plantit) and [`plantit-action`](https://github.com/Computational-Plant-Science/plantit-action).

## Requirements

- Python 3.8+

## Installation

To install with pip:

```shell
pip install pycyapi
```

## Usage

 Generally, the CLI is invoked with `pycyapi <command>`. All commands return JSON except `pycyapi token ...` (see below), which returns the token in plain text.

### Authenticating

The `pycyapi` CLI must obtain an access token to authenticate with CyVerse. The token may be provided to commands via the `--token` parameter, or set as an environment variable `CYVERSE_TOKEN`. An access token can be obtained from the Terrain API by sending a request with basic auth headers (valid CyVerse username and password):

```shell
GET https://de.cyverse.org/terrain/token/cas
```

A `token` command (see below) is provided as convenient alternative to manually obtaining a token.

### Commands

To show available commands help run `pycyapi --help`. The following commands are available:

- `version`: Show the current `pycyapi` version.
- `token`: Retrieve a CyVerse authentication token.
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

To show usage information for a specific command, run `pycyapi <command> --help`.

#### Version

To show the current version of `pycyapi`, use the `version` command:

```shell
pycyapi version
```

#### Token

To request a CyVerse CAS authentication token, use the `token` command:

```shell script
pycyapi token --username <your CyVerse username> --password <your CyVerse password>
```

The token can be passed via `--token (-t)` argument to authenticate subsequent commands.

#### User

The `user` command can be used to retrieve public profile information for CyVerse users. For instance:

```shell
pycyapi user -t <token> wbonelli
```

#### List

To list the contents of a collection in the data store, use the `list` command. For instance:

```shell
pycyapi list -t <token> /iplant/home/shared/iplantcollaborative/testing_tools/
```

#### Stat

To view metadata for a particular collection or object in the data store, use the `stat` command. For instance:

```shell
pycyapi stat -t <token> /iplant/home/shared/iplantcollaborative/testing_tools/
```

#### Pull

To download a single file from the data store to the current working directory, simply provide its full path:

```shell
pycyapi pull -t <token> /iplant/home/shared/iplantcollaborative/testing_tools/cowsay/cowsay.txt
```

To download all files from the `/iplant/home/shared/iplantcollaborative/testing_tools/cowsay/` collection to the current working directory, just provide the collection path instead:

```shell
pycyapi pull -t <token> /iplant/home/shared/iplantcollaborative/testing_tools/cowsay/
```

Optional arguments are:

- `--local_path (-p)`: Local path to download files to
- `--include_pattern (-ip)`: File patterns to include (0+)
- `--force (-f)`: Whether to overwrite already-existing files

#### Push

To upload all files in the current working directory to the `/iplant/home/<my>/<directory/` in the CyVerse Data Store, use:

```shell
pycyapi push -t <token> /iplant/home/<username>/<collection>/
```

Optional arguments include:

- `--local_path (-p)`: Local path to download files to
- `--include_pattern (-ip)`: File patterns to include (0+)
- `--include_name (-in)`: File names to include (0+)
- `--exclude_pattern (-ep)`: File patterns to exclude (0+)
- `--exclude_name (-en)`: File names to exclude (0+)

To upload a single file to the data store, provide the `--local_path (-p)` argument. For instance:

```shell
pycyapi push -t <token> /iplant/home/<username>/<collection/ -p /my/local/file.txt
```

If only `include_...`s are provided, only the file patterns and names specified will be included. If only `exclude_...`s section are present, all files except the patterns and names specified will be included. If you provide both `include_...` and `exclude_...` sections, the `include_...` rules will first be applied to generate a subset of files, which will then be filtered by the `exclude_...` rules.

#### Exists

To determine whether a particular path exists in the data store, use the `exists` command. For instance, to check if a collection exists:

```shell
pycyapi exists -t <token> /iplant/home/<username>/<collection
```

The `--type` option can be provided with value `dir` or `file` to verify that the given path is of the specified type.

#### Create

To create a new collection, use the `create` command:

```shell
pycyapi create -t <token> /iplant/home/<username>/<new collection name>
```

#### Share

To share a file or collection with another user, use the `share` command:

```shell
pycyapi share -t <token> /iplant/home/<username>/<collection> --username <user to share with> --permission <'read' or 'write'>
```

Note that you must provide both the `--username` and `--permission` flags.

#### Unshare

To revoke another user's access to your file or collection, use the `unshare` command:

```shell
pycyapi unshare -t <token> /iplant/home/<username>/<collection> --username <username>
```

This applies to both `read` and `write` permissions for the specified user.

#### Tag

To set metadata for a given file object or collection in your data store, use the `tag` command:

```shell
pycyapi tag <data object ID> -t <token> -a k1=v1 -a k2=v2
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
pycyapi tags <data object ID> -t <token>
```

This will retrieve standard attributes by default. To retrieve iRODS attributes instead, use the `--irods (-i)` option.
