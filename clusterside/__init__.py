'''
    Cluster side code for Plant IT. This code is in charge of submitting jobs
    to the cluster queue as well as running the jobs.

    Note:
        clusterSide supports two extra WORKFLOW_CONFIG configuration parameters
        not documentation in the Plant IT cookiecutter docs:

        .. code-block:: python
            :caption: workflow.py

            WORKFLOW_CONFIG = {

                ... # Other parameters as defined in the cookiecutter docs.

                # pre_commands are bash commands run by clusterside before
                # starting the singularity container.
                #
                # Commands should be a list, as supported by python's
                # subprocess.run
                #
                # They are ran in the work directory for the JOB, the absolute
                # path to the current sample directory is provided in {workdir}.
                # "{workdir}" will be replaced with the full path in any of the
                # string within the pre_commands list.
                #
                # In the example below, an empty singularity container is
                # created in the the work directory for the sample
                # currently being processed.
                "pre_commands": ["singularity", "image.create", "{workdir}/file.img"],

                # Extra command-line flags passed to singularity exec
                # when the singularity container is started.
                #
                # Commands should be a list, as supported by python's
                # subprocess.run
                #
                # {workdir} is replaced as for "pre_commands"
                #
                # In the example below, the empty singularity container
                # created using the above "pre-commands" is overlayed
                # onto the workflow singularity container.
                "singularity_flags": ["--overlay", "{workdir}/file.img"]
            }

            These should only be used in special circumstances, and
            as such not documented in Plant IT cookiecutter for the average user.
'''
