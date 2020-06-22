"""
    Command line interface.

    **cluster --job "definition.json" --run "[local/jobqueue]":** Runs a PlantIT job described in the given file
    on the given executor.

"""
import os
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

    def run(self):
        """
        Runs a job.
        """

        try:
            self.server.update_status(
                pk=self.job.id,
                token=self.job.token,
                status=self.server.OK,
                description=f"Starting '{singularity.__name__}' job '{self.job.id}'")
            for event in execute_pipeline_iterator(
                    singularity,
                    run_config={
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

    def dask_jobqueue(self, type: str):
        """
        Submits a job to a queueing system with dask-jobqueue.

        Args:
            type (str): Queueing system.
                Currently supported:
                - 'pbs'
                - 'slurm'
        """

        try:
            # TODO support for more systems? HTCondor, what else?
            if not (type == "pbs" or type == "slurm"):
                raise ValueError(f"Queue type '{type}' not supported")

            self.server.update_status(self.job.id,
                                      self.job.token,
                                      self.server.OK,
                                      f"Starting {type} job '{self.job.id}'")

            run_config = {
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
            }

            if self.job.executor:
                for k, v in self.job.executor.items():
                    if k == "name":
                        continue
                    run_config['execution']['dask']['config']['cluster'][type][k] = v

            with open("run_config.yaml", "w") as config_file:
                yaml.dump(run_config, config_file)

            env = os.environ.copy()
            env["LC_ALL"] = "en_US.utf8"
            env["LANG"] = "en_US.utf8"
            ret = subprocess.run(
                ["dagster", "pipeline", "execute",
                 "-p", "singularity",
                 "-w", f"{os.environ.copy()['DAGSTER_HOME']}/workspace.yaml",
                 "-c", "run_config.yaml"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if ret.returncode != 0:
                raise JobException(
                    f"Non-zero exit code from {type} job '{self.job.id}': {ret.stderr.decode('utf-8') if ret.stderr else ret.stdout.decode('utf-8') if ret.stdout else 'Unknown error'}")

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
        cluster = Cluster(job)
        if executor['type'] == "local":
            cluster.run()
        elif executor['type'] == "slurm" or executor['type'] == "pbs":
            cluster.dask_jobqueue(type=executor['type'])
        else:
            raise ValueError(f"Unknown executor '{executor['type']}' (currently supported: 'local', 'slurm', 'pbs')")


if __name__ == "__main__":
    cli()
