Overview
=========

Clusterside handles running workflows on the web server.

Clusterside Run Flow
----------------------

#. User submits a job within Plant IT

#. Plant IT creates a work directory on the server for the job

#. Plant IT uploads the sample list (`samples.json`), workflow
   information (`workflow.json`) and the `process.py` file for the workflow.

#. Plant IT runs `clusterside submit` on the cluster

#. Clusterside creates a submit file and submits it to the cluster
   using `qsub`. If available, (`~/.clusterside/submit.sh`) is used as a
   template for the submit file. Plant IT is informed the job was
   submitted to the cluster.

   The submit file calls `clusterside run`

#. When the submit file is run by the cluster, it calls `clusterside run`

#. For each sample in `samples.json`, clusterside run:

   * Informs Plant IT that the sample is being analyzed.
   * Creates a folder inside the work directory with the name of the sample
   * Downloads the sample from the iRODS server into the created folder
   * Runs process.py, passing the sample path and arguments
   * Collects the return data from process.py and places it in the results.sqlite.
   * Informs Plant IT that the sample was analyzed.

#. Once all samples have been processed. Clusterside reads in results.sqlite
   and collates the results from all processes into a zip file that is uploaded
   back to the iRODS cluster, and sends the path to Plant IT.

All communication from clusterside to Plant IT is done via the Plant IT REST API.
