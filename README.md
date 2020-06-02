# PlantIT Clusterside [![Build Status](https://travis-ci.com/Computational-Plant-Science/plantit-clusterside.svg?branch=master)](https://travis-ci.com/Computational-Plant-Science/plantit-clusterside)

Workflow manager for PlantIT. Allows the PlantIT web platform to deploy jobs to compute resources.

**This project is under active development and is not yet stable.**

## Requirements

The following are required to run PlantIT Clusterside in a Unix environment:

- Python 3.6+
- [Singularity](https://sylabs.io/docs/)
- [iRODS iCommands](https://wiki.cyverse.org/wiki/display/DS/Setting+Up+iCommands)

## Documentation

Full documentation can be found [here](https://plant-it-clusterside.readthedocs.io/en/latest/).

Documentation can be built using:

```
cd docs
make html
```

## Installation

To install `plantit-clusterside` on a compute cluster, run:

```
pip3 install --user git+https://github.com/Computational-Plant-Science/plantit-clusterside
```

Clusterside expects [iRODS iCommands](https://wiki.cyverse.org/wiki/display/DS/Setting+Up+iCommands) to be installed and configured with the `iinit` command. Clusterside reads in the configuration created by `iinit` to connect to the iRODS server. Configuration information for CyVerse iRODS is available [here](https://wiki.cyverse.org/wiki/display/DS/Setting+Up+iCommands).

## Usage

PlantIT Clusterside provides two commands:

### `clusterside submit`

Creates a submit file containing the `clusterside run` command
and submits the file using qsub.

If the file `$HOME/.clusterside/submit.sh` exists, the `clusterside run`
command is appended to the bottom of that file, then submitted. This is
useful for setting cluster settings. An example `$HOME/.clusterside/submit.sh`
may look like:

```
#PBS -S /bin/bash
#PBS -q bucksch_q
#PBS -l nodes=1:ppn=1
#PBS -l walltime=1:00:00:00
#PBS -l mem=6gb

ml Python/3.6.4-foss-2018a

cd $PBS_O_WORKDIR
```

Note the loading of Python 3.6 required to execute `clusterside run` and
moving into the jobs work directory. These will most likely be required running
on any cluster.

#### Note:

On some types of ssh connections, installation does not put clusterside in the
path. If the cluster throwing a "clusterside not found" error when submitting
jobs. Try using the whole path of clusterside for submitting. This can be
found by logging in to the cluster as the user PlantIT uses to submit the jobs
and executing `which clusterside`

### `clusterside run`

Runs the analysis. This should be called inside a cluster job, as done by
`clusterside submit`.

See `tests/test.py` for an example of the files required for `clusterside run`.

## Tests

Before running tests, run `bootstrap.sh`, then bring test containers up with `docker-compose -f docker-compose.test.yml up`.

Unit tests can then be run using: `docker-compose -f docker-compose.test.yml exec cluster /opt/plantit-clusterside/unit-tests.sh`

Integration tests can be run with `docker-compose -f docker-compose.test.yml exec cluster /opt/plantit-clusterside/integration-tests.sh`.
