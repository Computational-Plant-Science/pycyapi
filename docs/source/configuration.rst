Configuration
==============

.. _configuration-submit-template:

Submit file template
---------------------
Clusterside looks for a submit file template at `$HOME/.clusterside/submit.sh`.
If this file is present, clusterside will append the run commands (`clusterside run`)
to the end of the file.

An example `$HOME/.clusterside/submit.sh`
may look like:

.. code-block:: bash
  :caption: ~/.clusterside/submit.sh

  #PBS -S /bin/bash
  #PBS -q bucksch_q
  #PBS -l nodes=1:ppn=1
  #PBS -l walltime=1:00:00:00
  #PBS -l mem=6gb

  ml Python/3.6.4-foss-2018a

  cd $PBS_O_WORKDIR

.. Note::
  Note the loading of Python 3.6 required to execute `clusterside run` and
  moving into the jobs work directory. These will most likely be required running
  on any cluster.

Using the above template, the final `submit.sh` file created by
`clusterside submit` is

.. code-block:: bash
  :caption: submit.sh

  #PBS -S /bin/bash
  #PBS -q bucksch_q
  #PBS -l nodes=1:ppn=1
  #PBS -l walltime=1:00:00:00
  #PBS -l mem=6gb

  ml Python/3.6.4-foss-2018a

  cd $PBS_O_WORKDIR

  clusterside run

Remote (iRODS) File System Configuration
-----------------------------------------

Clusterside currently only supports one remote file system to download
sample files from. The remote file system should be configured on the
cluster using the irods icommand `iinit` under the user which clusterside
is run.

One configured with `iinit`, clusterside will automatically load the
configuration details required to connect to the configured
iRODS file system.
