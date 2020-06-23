import argparse
import json
import sys
import traceback

from dagster import execute_pipeline_iterator, DagsterEventType, DagsterInstance, reconstructable

from plantit_cluster.comms import Comms, STDOUTComms, RESTComms
from plantit_cluster.dagster.pipelines import *
from plantit_cluster.job import Job


class Cluster:

    @staticmethod
    def __comms(job: Job) -> Comms:
        return STDOUTComms() if not job.server else RESTComms(
            url=job.server,
            headers={"Authorization": "Token " + job.token})

    @staticmethod
    def run_inprocess(job: Job):
        """
        Runs a job in-process.

        Args:
            job: The job definition.
        """

        comms = Cluster.__comms(job)

        try:
            comms.update_status(
                pk=job.id,
                token=job.token,
                status=comms.OK,
                description=f"Starting '{singularity.__name__}' job '{job.id}'")
            for event in execute_pipeline_iterator(
                    singularity,
                    run_config={
                        'solids': {
                            'sources': {
                                'inputs': {
                                    'job': {
                                        "id": job.id,
                                        "workdir": job.workdir,
                                        "token": job.token,
                                        "server": job.server,
                                        "container": job.container,
                                        "commands": ' '.join(job.commands)
                                    }
                                }
                            }
                        },
                        'storage': {
                            'filesystem': {
                                'config': {
                                    'base_dir': job.workdir
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
                comms.update_status(
                    pk=job.id,
                    token=job.token,
                    status=comms.WARN if event.is_failure else comms.OK,
                    description=f"Dagster event '{event.event_type}' with message '{event.message}'")
                if event.event_type is DagsterEventType.PIPELINE_INIT_FAILURE or event.is_pipeline_failure:
                    raise JobException(event.message)
            comms.update_status(
                pk=job.id,
                token=job.token,
                status=comms.OK,
                description=f"Completed local job '{job.id}'")
        except Exception:
            comms.update_status(
                pk=job.id,
                token=job.token,
                status=comms.FAILED,
                description=f"Local job '{job.id}' failed: {traceback.format_exc()}")
            return

    @staticmethod
    def run_jobqueue(job: Job):
        """
        Runs a job on an HPC/HTC queueing system with dask-jobqueue.

        Args:
            job: The job definition.
        """

        comms = Cluster.__comms(job)
        queue_type = job.executor['name']

        try:
            # TODO support for more queueing systems? HTCondor, what else?
            if not (queue_type == "pbs" or queue_type == "slurm"):
                raise ValueError(f"Queue type '{queue_type}' not supported")

            comms.update_status(job.id,
                                job.token,
                                comms.OK,
                                f"Starting {queue_type} job '{job.id}'")

            run_config = {
                'solids': {
                    'sources': {
                        'inputs': {
                            'job': {
                                "id": job.id,
                                "workdir": job.workdir,
                                "token": job.token,
                                "server": job.server,
                                "container": job.container,
                                "commands": ' '.join(job.commands)
                            }
                        }
                    }
                },
                'execution': {
                    'dask': {
                        'config': {
                            'cluster': {
                                queue_type: {}
                            }
                        }
                    }
                },
                'storage': {
                    'filesystem': {
                        'config': {
                            'base_dir': job.workdir
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

            if job.executor:
                for k, v in job.executor.items():
                    if k == "name":
                        continue
                    run_config['execution']['dask']['config']['cluster'][queue_type][k] = v
            else:
                raise JobException("Expected root-level 'executor' section in JSON job definition")

            for event in execute_pipeline_iterator(reconstructable(singularity),
                                                   run_config=run_config,
                                                   instance=DagsterInstance.get()):
                comms.update_status(
                    pk=job.id,
                    token=job.token,
                    status=comms.WARN if event.is_failure else comms.OK,
                    description=f"Dagster event '{event.event_type}' with message '{event.message}'")
                if event.event_type is DagsterEventType.PIPELINE_INIT_FAILURE or event.is_pipeline_failure:
                    raise JobException(event.message)

            comms.update_status(job.id, job.token, comms.OK,
                                f"Completed {queue_type} job '{job.id}'")
        except Exception:
            comms.update_status(job.id, job.token, comms.FAILED,
                                f"{queue_type} job '{job.id}' failed: {traceback.format_exc()}")
            return


def cli():
    parser = argparse.ArgumentParser(description="PlantIT workflow management API.")
    _, args = parser.parse_known_args(sys.argv[1:])
    parser.add_argument('--job',
                        type=str,
                        help="JSON job definition file")
    opts = parser.parse_args(args)

    with open(opts.job) as file:
        job = Job(**json.load(file))

        if job.executor['name'] == "local":
            Cluster.run_inprocess(job)
        elif job.executor['name'] == "slurm" or job.executor['name'] == "pbs":
            Cluster.run_jobqueue(job)
        else:
            raise ValueError(f"Unknown executor '{job.executor['name']}' (currently supported: 'local', 'slurm', 'pbs')")


if __name__ == "__main__":
    cli()
