Plant IT Clusterside
=====================

Cluster side is started by the Plant IT web app to handle cluster-side
operations.

.. Attention::
  Python 3.6 or greater is required to run clusterside commands.

.. Attention::
  Clusterside currently only supports job submitting via the PBS `qsub` command.

.. Attention::
  Clusterside currently only supports 1 remote file system, the one configured via
  the irods icommand `iinit` for the user under which clusterside is run.

.. Note::
  clusterside supports two undocumented `WORKFLOW_CONFIG` params:

  * `pre_commands`: run command line programs before starting the singularity
    container
  * `singularity_flags`: pass extra flags to `singularity exec`

  see :mod:`clusterside` for more details.

.. toctree::
   :maxdepth: 2

   overview
   configuration

Full API
=========

 .. toctree::
    :maxdepth: 2

    clusterside

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
