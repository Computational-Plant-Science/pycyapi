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
import traceback

from clusterside.comms import Comms, STDOUTComms, RESTComms
from dagster import execute_pipeline_iterator
from dask.distributed import Client
from dask_jobqueue import PBSCluster, SLURMCluster

from clusterside.dagster.pipelines import *
from clusterside.job import Job


class Executor:
    def __init__(self, job: Job, server: Comms = STDOUTComms()):
        self.job = job
        self.server = server if not self.job.server else RESTComms(url=self.job.server, headers={
            "Authorization": "Token " + job.token})

    def local(self):
        """
        Submit a job in-process.
        """

        try:
            self.server.update_status(
                pk=self.job.id,
                token=self.job.token,
                status=self.server.OK,
                description=f"Starting local '{singularity.__name__}' job '{self.job.id}'")
            for event in execute_pipeline_iterator(
                singularity,
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
                    'loggers': {
                        'console': {
                            'config': {
                                'log_level': 'INFO'
                            }
                        }
                    }
                }):
                self.server.update_status(
                    pk=self.job.id,
                    token=self.job.token,
                    status=self.server.WARN if event.is_failure else self.server.OK,
                    description=f"{event.event_type} with message: '{event.message}'")
                if event.is_pipeline_failure:
                    raise JobException(event.message)
            self.server.update_status(
                pk=self.job.id,
                token=self.job.token,
                status=self.server.OK,
                description=f"Completed local '{singularity.__name__}' job '{self.job.id}'")
        except Exception:
            self.server.update_status(
                pk=self.job.id,
                token=self.job.token,
                status=self.server.FAILED,
                description=f"Local '{singularity.__name__}' job '{self.job.id}' failed: {traceback.format_exc()}")
            return

    def jobqueue(self,
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
        Submit a job to a resource management system with dask-jobqueue.

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
            self.server.update_status(self.job.id, self.job.token, self.server.OK, f"Starting {singularity.__name__} job '{self.job.id}' on dask-jobqueue scheduler '{client.scheduler}'")
            for event in execute_pipeline_iterator(
                singularity,
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
                    'execution': {
                        'dask': {
                            'config': {
                                'address': client.scheduler.address
                            }
                        }
                    },
                    'loggers': {
                        'console': {
                            'config': {
                                'log_level': 'INFO'
                            }
                        }
                    }
                }):
                self.server.update_status(
                    self.job.id,
                    self.job.token,
                    self.server.OK if event.is_successful_output else self.server.WARN,
                    f"Dagster pipeline event: '{event.event_type}'")
                if not event.is_successful_output:
                    raise JobException(event)
            self.server.update_status(self.job.id, self.job.token, self.server.OK, f"Completed {singularity.__name__} job '{self.job.id}' on dask-jobqueue scheduler '{client.scheduler}'")
        except Exception:
            self.server.update_status(self.job.id, self.job.token, self.server.FAILED, f"Dask-jobqueue {singularity.__name__} job '{self.job.id}' failed: {traceback.format_exc()}")
            return


def cli():
    parser = argparse.ArgumentParser(description="PlantIT's job orchestration CLI")
    parser.add_argument('cmd', type=str)
    cmds, args = parser.parse_known_args(sys.argv[1:])
    parser.add_argument('--file',
                        type=str,
                        help="JSON job definition file")
    opts = parser.parse_args(args)

    with open(opts.job) as file:
        job = json.load(file)
        cluster = Executor(job)
        if cmds.cmd == "local":
            cluster.local()
        elif cmds.cmd == "jobqueue":
            # TODO read from config file
            cluster.jobqueue(
                "slurm",
                1,
                "1GB",
                1,
                "batch",
                job.workdir,
                "00:00:01")
        else:
            raise ValueError(f"Unsupported command: {cmds.cmd}")


if __name__ == "__main__":
    cli()
