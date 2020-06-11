"""
    Command line interface.

    **cluster --job "definition.json" --run "[local/jobqueue]":** Runs a PlantIT job described in the given file
    on the given executor.

"""
import argparse
import json
import sys
import traceback
import yaml

from plantit_cluster.comms import Comms, STDOUTComms, RESTComms
from dagster import execute_pipeline_iterator, DagsterEventType
from dask.distributed import Client
from dask_jobqueue import PBSCluster, SLURMCluster

from plantit_cluster.dagster.pipelines import *
from plantit_cluster.job import Job


class Cluster:
    def __init__(self, job: Job, server: Comms = STDOUTComms()):
        self.job = job
        self.server = server if not self.job.server else RESTComms(url=self.job.server, headers={
            "Authorization": "Token " + job.token})

    def local(self):
        """
        Runs a job in-process.
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
                    description=f"Dagster event '{event.event_type}' with message: '{event.message}'")
                if event.event_type is DagsterEventType.PIPELINE_INIT_FAILURE or event.is_pipeline_failure:
                    raise JobException(event.message)
            self.server.update_status(
                pk=self.job.id,
                token=self.job.token,
                status=self.server.OK,
                description=f"Completed local job '{self.job.id}'")
        except Exception:
            self.server.update_status(
                pk=self.job.id,
                token=self.job.token,
                status=self.server.FAILED,
                description=f"Local job '{self.job.id}' failed: {traceback.format_exc()}")
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
        Submits a job to a resource management system with dask-jobqueue.

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
            # TODO support for more resource managers? HTCondor, what else?
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
                    local_directory=local_directory,
                    walltime=walltime,
                    job_extra=['-p "debug"'])
            else:
                raise ValueError(f"Queue type '{queue_type}' not supported")

            cluster.adapt(minimum_jobs=min_jobs, maximum_jobs=max_jobs)
            client = Client(cluster)
            self.server.update_status(self.job.id,
                                      self.job.token,
                                      self.server.OK,
                                      f"Starting jobqueue job '{self.job.id}' on dask scheduler '{client.scheduler}'")

            config = {
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
                'execution': {
                    'dask': {
                        'config': {
                            'address': client.scheduler.address
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
            }

            with open("dask.yaml", "w") as config_file:
                yaml.dump(config, config_file)

            ret = subprocess.run(["dagster", "pipeline", "execute", "singularity", "-y", "plantit_cluster/dagster/repository.yaml", "-e", "dask.yaml"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if ret.returncode != 0:
                raise JobException(f"Non-zero exit code from Dagster pipeline for job '{self.job.id}': {ret.stderr.decode('utf-8') if ret.stderr else ret.stdout.decode('utf-8') if ret.stdout else 'Unknown error'}")

            self.server.update_status(self.job.id, self.job.token, self.server.OK,
                                      f"Completed jobqueue job '{self.job.id}' on dask scheduler '{client.scheduler}'")
        except Exception:
            self.server.update_status(self.job.id, self.job.token, self.server.FAILED,
                                      f"Jobqueue job '{self.job.id}' failed: {traceback.format_exc()}")
            return


def cli():
    parser = argparse.ArgumentParser(description="PlantIT workflow management API.")
    _, args = parser.parse_known_args(sys.argv[1:])
    parser.add_argument('--job',
                        type=str,
                        help="JSON job definition file")
    parser.add_argument('--run',
                        type=str,
                        help="How to run the job (currently supported: 'local', 'jobqueue')")
    opts = parser.parse_args(args)

    with open(opts.job) as file:
        job_json = json.load(file)
        job = Job(
            id=job_json['id'],
            token=job_json['token'],
            workdir=job_json['workdir'],
            server=job_json['server'] if 'server' in job_json else None,
            container=job_json['container'],
            commands=str(job_json['commands']).split())
        # TODO pass job to run command rather than at init time - cluster should be persistent
        cluster = Cluster(job)
        if opts.run == "local":
            cluster.local()
        elif opts.run == "jobqueue":
            # TODO read from config file
            cluster.jobqueue(
                "slurm",
                1,
                "250MB",
                1,
                "",
                job.workdir,
                "00:00:10")
        else:
            raise ValueError(f"Unknown run configuration '{opts.run}' (currently supported: 'local', 'jobqueue')")


if __name__ == "__main__":
    cli()
