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
                 type: str,
                 min_jobs: int = 1,
                 max_jobs: int = 10):
        """
        Submits a job to a resource management system with dask-jobqueue.

        Args:
            type (str): Queueing system.
                Currently supported:
                - 'pbs'
                - 'slurm'
            min_jobs (int): Minimum concurrent jobs.
            max_jobs (int): Maximum concurrent jobs.
        """

        try:
            # TODO support for more queueing systems? HTCondor, what else?
            if not (type == "pbs" or type == "slurm"):
                raise ValueError(f"Queue type '{type}' not supported")

            self.server.update_status(self.job.id,
                                      self.job.token,
                                      self.server.OK,
                                      f"Starting {type} job '{self.job.id}'")

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
                            'cluster': {
                                type: {}
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
                },
                # TODO (optional) remote Dagit run launcher
            }

            if self.job.executor:
                for k, v in self.job.executor.items():
                    if k == "name":
                        continue
                    config['execution']['dask']['config']['cluster'][type][k] = v

            with open("dask.yaml", "w") as config_file:
                yaml.dump(config, config_file)

            ret = subprocess.run(
                ["dagster", "pipeline", "execute", "-p", "singularity", "-w", "plantit_cluster/dagster/workspace.yaml",
                 "-c", "dask.yaml"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if ret.returncode != 0:
                raise JobException(
                    f"Non-zero exit code from Dagster pipeline for job '{self.job.id}': {ret.stderr.decode('utf-8') if ret.stderr else ret.stdout.decode('utf-8') if ret.stdout else 'Unknown error'}")

            self.server.update_status(self.job.id, self.job.token, self.server.OK,
                                      f"Completed {type} job '{self.job.id}'")
        except Exception:
            self.server.update_status(self.job.id, self.job.token, self.server.FAILED,
                                      f"{type} job '{self.job.id}' failed: {traceback.format_exc()}")
            return


def cli():
    parser = argparse.ArgumentParser(description="PlantIT workflow management API.")
    _, args = parser.parse_known_args(sys.argv[1:])
    parser.add_argument('--job',
                        type=str,
                        help="JSON job definition file")
    opts = parser.parse_args(args)

    with open(opts.job) as file:
        job_json = json.load(file)
        executor = job_json['executor']
        job = Job(
            id=job_json['id'],
            token=job_json['token'],
            workdir=job_json['workdir'],
            server=job_json['server'] if 'server' in job_json else None,
            container=job_json['container'],
            commands=str(job_json['commands']).split(),
            executor=executor)
        # TODO pass job to run command rather than at init time - cluster should be persistent
        cluster = Cluster(job)
        if executor['name'] == "local":
            cluster.local()
        elif executor['name'] == "slurm" or executor['name'] == "pbs":
            cluster.jobqueue(type=executor['name'])
        else:
            raise ValueError(f"Unknown executor '{executor['name']}' (currently supported: 'local', 'slurm', 'pbs')")


if __name__ == "__main__":
    cli()
