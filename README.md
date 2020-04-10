# Plant IT Clusterside

[![Build Status](https://travis-ci.com/Computational-Plant-Science/DIRT2_ClusterSide.svg?branch=master)](https://travis-ci.com/Computational-Plant-Science/DIRT2_ClusterSide)

This repository contains the code that submits and runs jobs on compute
clusters using qsub and collects the results to return them back to the
PlantIT web server.

Cluster Installation
---------------------
To install clusterside on a cluster, run:

```
pip3 install --user git+https://github.com/Computational-Plant-Science/DIRT2_ClusterSide
```

on the cluster under the the user that Plant IT will use to login and submit jobs.

Clusterside expects [irods icommands](https://wiki.cyverse.org/wiki/display/DS/Setting+Up+iCommands) to be installed and irods already
configured to connect to the file server using the `iinit` command. Clusterside
reads in the configuration created by `iinit` to connect to the iRODS server. Configuration information for CyVerse iRODS is available [here](https://wiki.cyverse.org/wiki/display/DS/Setting+Up+iCommands).

#### Note:
Clusterside currently only supports one file system. The one configured to work with icommands.

Documentation
---------

Full Documentation: [Link](https://plant-it-clusterside.readthedocs.io/en/latest/)

Documentation can be built using

```
cd docs
make html
```

Clusterside provides two commands:

## `clusterside submit`
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

## `clusterside run`
Runs the analysis. This should be called inside a cluster job, as done by
`clusterside submit`.

See `tests/test.py` for an example of the files required for `clusterside run`.

# tests
Tests are written in pytest and can be run usng: `python3 -m pytest`

The `python3 test/test.py` script can also be run to test clusterside, but requires
irods to be setup on the system, and the sample file paths to exist on the irods server
