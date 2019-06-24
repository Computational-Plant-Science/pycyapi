Plant IT Clusterside
=====================

Cluster side is started by the Plant IT web app to handle cluster-side
operations.

.. Attention::
  Python 3.6 or greater is required to run clusterside commands.

.. Attention::
  Clusterside only supports job submitting via the PBS `qsub` command.

.. Attention::
  Clusterside only 1 remote file system, the one configured via
  the irods icommand `iinit` for the user under which clusterside is run. 


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
