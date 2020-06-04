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
from clusterside.job import Job


class Clusterside:
    job = None
    server = None

    def __init__(self, job: Job, mock_comms: bool = False):
        self.job = job
        self.server = STDOUTComms() if mock_comms or not self.job.server else RESTComms(url=self.job.server, headers={
            "Authorization": "Token " + job.token})

    def in_process(self):
        """
        Submit a job in-process.
        """

        try:
            msg = f"Starting job '{self.job.id}' (in-process)"
            print(msg)
            self.server.update_status(self.job.id, self.job.token, self.server.OK, msg)
            execute_pipeline(
                singularity_job,
                environment_dict={
                    'solids': {
                        'sources': {
                            'inputs': {
                                'job': {
                                    "id": self.job.id,
                                    "workdir": self.job.workdir,
                                    "token": self.job.token,
                                    "server": self.job.server,
                                    "container": self.job.container,
                                    "commands": ' '.join(self.job.commands)
                                }
                            }
                        }
                    },
                    'storage': {
                        'filesystem': {
                            'config': {
                                'base_dir': self.job.workdir
                            }
                        }
                    },
                })
            msg = f"Completed job '{self.job.id}' (in-process)"
            print(msg)
            self.server.update_status(self.job.id, self.job.token, self.server.OK, msg)
        except Exception:
            msg = f"Job '{self.job.id}' (in-process) failed: {traceback.format_exc()}"
            print(msg)
            self.server.update_status(self.job.id, self.job.token, self.server.FAILED, msg)
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
        Submit a job to a resource management system supported by Dask-Jobqueue.

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
            client = Client(cluster)
            msg = f"Starting job '{self.job.id}' (dask-distributed scheduler '{client.scheduler}')"
            print(msg)
            self.server.update_status(self.job.id, self.job.token, self.server.OK, msg)
            execute_pipeline(
                singularity_job,
                environment_dict={
                    'solids': {
                        'extract_inputs': {
                            'inputs': {
                                'job': json.loads(json.dumps(self.job))
                            }
                        }
                    },
                    'storage': {
                        'filesystem': {
                            'config': {
                                'base_dir': self.job.workdir
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
            msg = f"Completed job '{self.job.id}' (dask-jobqueue)"
            print(msg)
            self.server.update_status(self.job.id, self.job.token, self.server.OK, msg)
        except Exception:
            msg = f"Job '{self.job.id}' (dask-jobqueue) failed: {traceback.format_exc()}"
            print(msg)
            self.server.update_status(self.job.id, self.job.token, self.server.FAILED, msg)
            return


def cli():
    parser = argparse.ArgumentParser(description="PlantIT's job orchestration CLI")
    parser.add_argument('cmd', type=str, help='What to do')
    args, unknownargs = parser.parse_known_args(sys.argv[1:])
    parser.add_argument('--id',
                        type=str,
                        help="Unique identifier")
    parser.add_argument('--token',
                        type=str,
                        help="Authentication token")
    parser.add_argument('--workdir',
                        type=str,
                        help="Working directory")
    parser.add_argument('--server',
                        type=str,
                        help="PlantIT web API URL")
    parser.add_argument('--container',
                        type=str,
                        help="Docker Hub or Singularity Hub container URL")
    parser.add_argument('--commands',
                        type=str,
                        default="echo 'Hello, world!'",
                        help="Commands to execute in container")

    if args.cmd == "local":
        opts = parser.parse_args(unknownargs)
        workflow = Job(opts.id, opts.token, opts.workdir, opts.server, opts.container, opts.commands)
        cluster = Clusterside(workflow)
        cluster.in_process()
    elif args.cmd == "jobqueue":
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
        workflow = Job(opts.id, opts.token, opts.workdir, opts.server, opts.container, opts.commands)
        cluster = Clusterside(workflow)
        cluster.dask_jobqueue(opts.script, opts.type, opts.cores, opts.memory, opts.processes, opts.queue,
                              opts.directory, opts.walltime)
    else:
        raise ValueError(f"Unsupported command: {args.cmd}")


if __name__ == "__main__":
    cli()
