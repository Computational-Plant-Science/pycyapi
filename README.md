# PyDE

A Python client for the CyVerse Discovery Environment (Terrain) API.

##This repository is under active development and is not yet stable. Coverage will not approach a 1-1 mapping for some time.**

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Contents**

- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
  - [Authenticating](#authenticating)
  - [Commands](#commands)
    - [Pull](#pull)
    - [Push](#push)
- [Development](#development)
  - [Tests](#tests)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Requirements

- Python 3.6.9+

## Installation

To install with  pip:

```
pip install plantit-cli
```

## Usage

Once the CLI is installed it can be invoked with `pyde <command>`.

### Authenticating

The `plantit terrain pull` and `push` commands use the Terrain API to access the CyVerse Data Store. As such you must provide a `--terrain_token` argument. For instance:

```shell
pyde pull /iplant/home/user/collection/ --terrain_token <token>
```

An access token can be obtained from the Terrain API (providing username/password for basic auth):

```shell script
GET https://de.cyverse.org/terrain/token/cas
```

### Commands

The following commands are supported:

- `pull`: Download files from the CyVerse Data Store.
- `push`: Upload files to the CyVerse Data Store.

The `pull` and `push` commands provide a high-level interface over Terrain's `terrain/secured/fileio` endpoints, supporting directory or file paths as well as concurrent requests for large uploads and downloads.

#### Pull

To pull files from the `/iplant/home/shared/iplantcollaborative/testing_tools/cowsay/` directory in the CyVerse Data Store to the current working directory, use:

```shell
plantit terrain pull /iplant/home/shared/iplantcollaborative/testing_tools/cowsay/ --terrain_token <token>
```

Optional arguments are:

- `--local_path (-p)`: Local path to download files to.
- `--pattern`: File patterns to include (one or more).
- `--overwrite`: Whether to overwrite already-existing files.


#### Push

To push files in the current working directory to the `/iplant/home/<my>/<directory/` in the CyVerse Data Store, use `plantit terrain push /iplant/home/<my>/<directory/ --terrain_token <token>`.

Options are:

- `--local_path (-p)`: Local path to download files to.
- `--include_pattern (-ip)`: File patterns to include (one or more).
- `--include_name (-in)`: File names to include (one or more).
- `--exclude_pattern (-ep)`: File patterns to exclude (one or more).
- `--exclude_name (-en)`: File names to exclude (one or more).

If only `include_...`s are provided, only the file patterns and names specified will be included. If only `exclude_...`s section are present, all files except the patterns and names specified will be included. If you provide both `include_...` and `exclude_...` sections, the `include_...` rules will first be applied to generate a subset of files, which will then be filtered by the `exclude_...` rules.

## Development

To set up a development environment, clone the repo with `git clone https://github.com/Computational-Plant-Science/pyde.git`.

### Tests

TODO
