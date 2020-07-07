import json
import traceback

from dagster import execute_pipeline_iterator, reconstructable, DagsterInstance, DagsterEventType

from plantit_cluster.dagster.solids import construct_pipeline_with_input_files
from plantit_cluster.exceptions import PlantitException
from plantit_cluster.executor.executor import Executor
from plantit_cluster.input.irodsinput import IRODSInput
from plantit_cluster.pipeline import Pipeline


class JobQueueExecutor(Executor):
    """
    Runs pipelines on an HPC/HTC queueing system with dask-jobqueue.
    """

    name = None

    def __init__(self, **kwargs):
        self.name = kwargs["name"]
        self.kwargs = kwargs

    def execute(self, pipeline: Pipeline):
        """
        Runs a pipeline on an HPC/HTC queueing system with dask-jobqueue.

        Args:
            pipeline: The pipeline definition.
        """

        try:
            irods = IRODSInput(
                path=pipeline.input.path,
                host=pipeline.input.host,
                port=pipeline.input.port,
                user=pipeline.input.user,
                password=pipeline.input.password,
                zone=pipeline.input.zone,
            )
            files = irods.list()
            print(f"Pulling input files {files}")
            irods.pull(pipeline.workdir)
            print(f"Successfully pulled input files {files}")

            # TODO support for more queueing systems? HTCondor, what else?
            if not (self.name == "pbs" or self.name == "slurm"):
                raise ValueError(f"Queue type '{self.name}' not supported")

            print(f"Running {self.name} pipeline")

            run_config = {
                'execution': {
                    'dask': {
                        'config': {
                            'cluster': {
                                self.name: {}
                            }
                        }
                    }
                },
                'storage': {
                    'filesystem': {
                        'config': {
                            'base_dir': pipeline.workdir
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

            for k, v in self.kwargs.items():
                if k == "name":
                    continue
                run_config['execution']['dask']['config']['cluster'][self.name][k] = v

            for event in execute_pipeline_iterator(
                    construct_pipeline_with_input_files(pipeline, files),
                    run_config=run_config,
                    instance=DagsterInstance.get()):
                print(f"Dagster event '{event.event_type}' with message '{event.message}'")
                if event.event_type is DagsterEventType.PIPELINE_INIT_FAILURE or event.is_pipeline_failure:
                    raise PlantitException(event.message)

            print(f"Successfully ran {self.name} pipeline")
        except Exception:
            print(f"Failed to run {self.name} pipeline: {traceback.format_exc()}")
            return
