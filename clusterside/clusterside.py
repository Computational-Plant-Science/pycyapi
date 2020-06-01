"""
    Command line interface and logic.

    Provides the command line commands of:

    **clusterside submit:** Creates a submit file containing the
    `clusterside run` command and submits the file using qsub.

    **clusterside run:** Runs the analysis. This should be called inside a
    cluster job, as done by `clusterside submit`.
"""
import argparse
import json
import sys

from dagster import execute_pipeline
from dask.distributed import Client
from dask_jobqueue import PBSCluster, SLURMCluster

from clusterside.dagster.pipelines import *


class ClusterSide:
    workflow = {}
    server = None

    def __init__(self, workflow_file="workflow.json", ):
        with open(workflow_file, 'r') as w:
            self.workflow = json.load(w)

        self.server = RESTComms(url=self.workflow['server_url'],
                                headers={
                                    "Authorization": "Token " + self.workflow['token']
                                },
                                job_pk=self.workflow['job_pk'])

    def __dask(self, client: Client):
        self.server.update_status(self.server.OK,
                                  f"Started Dask scheduler at '{client.scheduler}'... Starting job '{self.workflow['job_pk']}'")
        execute_pipeline(
            singularity_workflow,
            environment_dict={
                'solids': {
                    'extract_inputs': {
                        'inputs': {
                            'workflow': self.workflow
                        }
                    }
                },
                'storage': {
                    'filesystem': {
                        'config': {
                            'base_dir': self.workflow['work_dir']
                        }
                    }
                },
                'execution': {
                    'dask': {
                        'config': {
                            'address': client.scheduler
                        }
                    }
                }
            })
        self.server.update_status(self.server.OK, f"Job '{self.workflow['job_pk']}' completed successfully")

    def dask_local(self):
        """
        Submit a job on a local Dask-Distributed cluster.
        """

        try:
            self.__dask(Client())
        except Exception:
            print(f"Job '{self.workflow['job_pk']}' failed: {traceback.format_exc()}")
            self.server.update_status(self.server.FAILED,
                                      f"Job '{self.workflow['job_pk']}' failed: {traceback.format_exc()}")
            return

    def dask_jobqueue(self,
                      queue_type: str,
                      cores: int,
                      memory: str,
                      processes: int,
                      queue: str,
                      local_directory: str,
                      walltime: str,
                      min_jobs: int = 1,
                      max_jobs: int = 10):
        """
        Submit a job to cluster management system supported by Dask-Jobqueue.

        Args:
            queue_type (str): Cluster queueing system.
                Currently supported:
                - 'pbs'
                - 'slurm'
            cores (int): Cores per node.
            memory (str): RAM per node.
            processes (int): Processes per core.
            queue (str): Queue name.
            local_directory (str): Local working (or scratch) directory.
            walltime (str): Maximum job walltime.
            min_jobs (int): Minimum concurrent jobs.
            max_jobs (int): Maximum concurrent jobs.
        """

        try:
            if queue_type.lower() == "pbs":
                cluster = PBSCluster(
                    cores=cores,
                    memory=memory,
                    processes=processes,
                    queue=queue,
                    local_directory=local_directory,
                    walltime=walltime)
            elif queue_type.lower() == "slurm":
                cluster = SLURMCluster(
                    cores=cores,
                    memory=memory,
                    processes=processes,
                    queue=queue,
                    local_directory=local_directory,
                    walltime=walltime)
            else:
                raise ValueError(f"Queue type '{queue_type}' not supported")

            cluster.adapt(minimum_jobs=min_jobs, maximum_jobs=max_jobs)
            self.__dask(Client(cluster))
        except Exception:
            print(f"Job '{self.workflow['job_pk']}' failed: {traceback.format_exc()}")
            self.server.update_status(self.server.FAILED,
                                      f"Job '{self.workflow['job_pk']}' failed: {traceback.format_exc()}")
            return


def cli():
    parser = argparse.ArgumentParser(description="PlantIT's workflow orchestration CLI")
    parser.add_argument('cmd', type=str, help='What to do')
    args, unknownargs = parser.parse_known_args(sys.argv[1:])
    main = ClusterSide()

    if args.cmd == "local":
        main.dask_local()
    elif args.cmd == "jobqueue":
        parser = argparse.ArgumentParser(description='Run a workflow')
        parser.add_argument('--type',
                            type=str,
                            default="pbs",
                            help="Cluster queueing system (.e.g, 'pbs', 'slurm')")
        parser.add_argument('--cores',
                            type=int,
                            default="1",
                            help="Cores per node")
        parser.add_argument('--memory',
                            type=str,
                            default="1GB",
                            help="RAM per node")
        parser.add_argument('--processes',
                            type=int,
                            default="1",
                            help="Processes per core")
        parser.add_argument('--queue',
                            type=str,
                            default="batch",
                            help="Queue name")
        parser.add_argument('--directory',
                            type=str,
                            required=True,
                            help="Local working directory")
        parser.add_argument('--walltime',
                            type=str,
                            default="01:00:00",
                            help="Job walltime")
        opts = parser.parse_args(unknownargs)

        main.dask_jobqueue(opts.script, opts.type, opts.cores, opts.memory, opts.processes, opts.queue, opts.directory, opts.walltime)
    else:
        raise ValueError(f"Unsupported command: {args.cmd}")


if __name__ == "__main__":
    cli()
