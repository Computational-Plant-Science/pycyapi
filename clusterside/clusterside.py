"""
    Command line interface and logic.

    Provides the command line commands of:

    **clusterside submit:** Creates a submit file containing the
    `clusterside run` command and submits the file using qsub.

    **clusterside run:** Runs the analysis. This should be called inside a
    cluster job, as done by `clusterside submit`.
"""
import stat
import os
import shutil
import argparse
import json
import sys

from clusterside import submitters
from clusterside.data import Collection, Workflow, upload_file
from clusterside.comms import RESTComms
from clusterside.executors import SingleJobExecutor


class ClusterSide:
    """
        Command line interface and logic.
    """

    # Configuration loaded form the Job config json file. Populated by main()
    config = {}

    # Server communication object
    server = None

    #

    def __init__(self, workflow_file="workflow.json", ):
        """
            Main Program Function
        """
        with open(workflow_file, 'r') as fin:
            self.config = json.load(fin)

        self.server = RESTComms(url=self.config['server_url'],
                                headers={
                                    "Authorization": "Token " + self.config['token']
                                },
                                job_pk=self.config['job_pk'])

    def submit(self, script_template, cluster_type):
        """
            Submit a job to the cluster

            Args:
                script_template (str): Path to the file to use as a template for
                    creating a the submission script.
                    (See :ref:`configuration-submit-template`)
                cluster_type (str): Type of cluster submitting to.
                    Must be one of:
                    - 'pbs': for PBS based clusters
                    - 'slurm': Slurm based clusters
        """
        script_name = "./submit_%d.sh" % (self.config['job_pk'],)

        template_path = os.path.expanduser(script_template)
        if os.path.isfile(template_path):
            shutil.copy(template_path, script_name)

        print(f"Submitting job '{self.config['job_pk']}'")

        with open(script_name, 'a+') as fout:
            fout.write("\n")
            fout.write("clusterside run")

        os.chmod(script_name,
                 stat.S_IRUSR | stat.S_IXUSR)

        try:
            if cluster_type == 'pbs':
                ret = submitters.pbs(script_name)
            elif cluster_type == 'slurm':
                ret = submitters.slurm(script_name)
            else:
                print(f"Job '{self.config['job_pk']}' submission failed: unknown cluster type {cluster_type}")
                self.server.update_status(self.server.FAILED,
                                          f"Job '{self.config['job_pk']}' submission failed: unknown cluster type {cluster_type}")
                return
        except FileNotFoundError as error:
            print(f"Job '{self.config['job_pk']}' submission failed: {error}")
            self.server.update_status(self.server.FAILED, str(error))
            exit()

        if ret.returncode == 0:
            self.server.update_status(self.server.OK, "Queued")

        if ret.stderr:
            self.server.update_status(self.server.FAILED, ret.stderr)

    def run(self):
        """
            Run the workflow analysis
        """

        try:
            collection = Collection("samples.json")
            workflow = Workflow("workflow.json")
            server = RESTComms(url=workflow.server_url,
                               headers={
                                   "Authorization": "Token " + workflow.token
                               },
                               job_pk=workflow.job_pk)

            executor = SingleJobExecutor(collection, workflow, server)
            self.server.update_status(self.server.OK, "Running")
            executor.process()
            results_path = executor.reduce()

            # TODO: This is to test the upload system, a more robust system,
            # supporting multiple file servers will need to be added later
            _, file_extension = os.path.splitext(results_path)
            remote_results_path = os.path.join(collection.base_file_path,
                                               "results_job%d%s" % (workflow.job_pk, file_extension))
            upload_file(results_path, remote_results_path)
            server.update_job({'remote_results_path': remote_results_path})

            self.server.task_complete(self.config['task_pk'])
        except Exception as error:
            self.server.update_status(self.server.FAILED, str(error))


def cli():
    """
        Clusterside Command line interface

        Called by default if clusterside is run from the command line.
    """
    parser = argparse.ArgumentParser(
        description='The Cluster Side Component of the DIRT2 Webplatform'
    )
    parser.add_argument('cmd', type=str,
                        help='what to do.')
    args, unknownargs = parser.parse_known_args(sys.argv[1:])

    main = ClusterSide()

    if args.cmd == "submit":
        parser = argparse.ArgumentParser(description='Submit a job to the cluster')
        parser.add_argument('--script', type=str,
                            default="~/.clusterside/submit.sh",
                            help='Script template location')
        parser.add_argument('--cluster_type', type=str,
                            default="pbs",
                            help='Type of cluster to submit to: pbs or slurm (default: pbs)')
        opts = parser.parse_args(unknownargs)

        main.submit(opts.script, opts.cluster_type)
    elif args.cmd == "run":
        main.run()


if __name__ == "__main__":
    cli()
