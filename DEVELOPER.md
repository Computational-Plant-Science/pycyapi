# Developing `pycyapi`

## Requirements

Python 3.8+ is required to develop `pycyapi`.

## Installation

First, clone the repo with `git clone https://github.com/Computational-Plant-Science/pycyapi.git`.

Create a Python3 virtual environment, e.g. `python -m venv venv`, then install `pycyapi` and core dependencies with `pip install .`. Install testing and linting dependencies as well with `pip install ".[test]".

## Running tests

The tests can be run from the project root with `pytest` (or `python -m pytest`). Use `-v` for verbose mode and `-n auto` to run them in parallel on as many cores as your machine will spare.

**Note:** some tests required the `CYVERSE_USERNAME` and `CYVERSE_PASSWORD` environment variables. You can set these manually or put them in a `.env` file in the project root &mdash; `pytest-dotenv` will detect them in the latter case. Test cases will use this CyVerse account and its associated data store as a test environment. Each test case isolates its workspace to a folder named by GUID.

### Test markers

The full test suite should take 5-10 minutes to run, depending on the delay configured to allow the CyVerse Data Store to become consistent. This is 10 seconds per write operation, by default. 

**Note:** The CyVerse data store is not immediately consistent and write operations may take some time to be reflected in subsequent reads. Tests must wait some unknown amount of time to allow the Data Store to update its internal state. If tests begin to fail intermittently, the `DEFAULT_SLEEP` variable in `pycyapi/terrain/tests/conftest.py` may need to be increased.

### Smoke tests

A fast subset of the tests can be run with `pytest -S` (short for `--smoke`). The smoke tests should complete in under a minute.

## Releases

TODO