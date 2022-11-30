# Developing `pycyapi`

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [Requirements](#requirements)
- [Installation](#installation)
- [Linting](#linting)
- [Testing](#testing)
  - [Environment variables](#environment-variables)
  - [Test markers](#test-markers)
  - [Smoke tests](#smoke-tests)
- [Releases](#releases)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Requirements

Python 3.8+ is required to develop `pycyapi`.

## Installation

First, clone the repo with `git clone https://github.com/Computational-Plant-Science/pycyapi.git`.

Create a Python3 virtual environment, e.g. `python -m venv venv`, then install `pycyapi` and core dependencies with `pip install .`. Install testing and linting dependencies as well with `pip install ".[test]".

## Linting

To lint Python source files, run `python scripts/lint.py`. This will run `black` and `isort` on all files in the `pycyapi` directory.

## Testing

This project's tests are contained within the top-level source directory `pycyapi`. The tests can be run from the project root with `pytest` (or `python -m pytest`). Use `-v` for verbose mode and `-n auto` to run tests in parallel on as many cores as your machine will spare.

### Environment variables

The test suite requires some environment variables:

- `CYVERSE_USERNAME`
- `CYVERSE_PASSWORD`
- `DATA_STORE_WRITE_OP_SLEEP`
- `TESTING_EMAIL`

You can set these manually (e.g. `export CYVERSE_USERNAME=<your username>` or `CYVERSE_USERNAME=<your username> ... pytest`), but a more convenient solution is to put them in a `.env` file in the project root, in which case `pytest-dotenv` will detect them and load them into the testing environment.

**Note:** test cases will subsequently use the provided CyVerse account and its Data Store allocation as a test environment. Each test case isolates its workspace to a folder named by GUID, and attempts to remove it afterwards. However, if a test case fails, the folder may not be removed &mdash; as such you may need to manually check for and remove test folders from the Data Store.

### Test markers

The full test suite should take 5-10 minutes to run, depending on the delay configured to allow the CyVerse Data Store to become consistent. This is 10 seconds per write operation, by default. 

**Note:** The CyVerse data store is not immediately consistent and write operations may take some time to be reflected in subsequent reads. Tests must wait some unknown amount of time to allow the Data Store to update its internal state. If tests begin to fail intermittently, the `DEFAULT_SLEEP` variable in `pycyapi/terrain/tests/conftest.py` may need to be increased.

### Smoke tests

A fast subset of the tests can be run with `pytest -S` (short for `--smoke`). The smoke tests should complete in under a minute.

## Releases

To create a `pycyapi` release candidate, create a branch from the tip of `develop` named `vX.Y.Zrc`, where `X.Y.Z` is the [semantic version](https://semver.org/) number. The `release.yml` CI workflow to build and test the release candidate, then draft a PR into `master`. To promote the candidate to an official release, merge the PR into `master`. This will trigger a final CI job to tag the release revision to `master`, rebase `master` on `develop`, publish the release to PyPI, and post the release notes to GitHub.