import json
import os
import traceback
from os.path import join

from dagster import execute_pipeline_iterator, reconstructable, DagsterInstance, DagsterEventType

from plantit_cluster.dagster.solids import construct_pipeline_with_input_files, construct_pipeline_with_input_directory, \
    construct_pipeline_with_no_input
from plantit_cluster.exceptions import PipelineException
from plantit_cluster.executor.executor import Executor
from plantit_cluster.store.irodsstore import IRODSStore
from plantit_cluster.run import Run


def object_hook(dct):
    if 'path' in dct:
        print(dct)
        return IRODSStore(**dct)
    return dct


class JobQueueExecutor(Executor):
    """
    Runs pipelines on an HPC/HTC queueing system with dask-jobqueue.
    """

    name = None

    def __init__(self, **kwargs):
        self.name = kwargs["name"]
        self.kwargs = kwargs

    def execute(self, pipeline: Run):
        """
        Runs a pipeline on an HPC/HTC queueing system with dask-jobqueue.

        Args:
            pipeline: The pipeline definition.
        """

        try:
            input_kind = None if pipeline.source is None else pipeline.source['kind']
            output_kind = None if pipeline.sink is None else pipeline.sink['kind']

            if input_kind is not None:
                inputStore = IRODSStore(
                    host=pipeline.source['host'],
                    port=pipeline.source['port'],
                    user=pipeline.source['user'],
                    password=pipeline.source['password'],
                    zone=pipeline.source['zone'],
                    path=pipeline.source['irods_path'])

            if output_kind is not None:
                outputStore = IRODSStore(
                    host=pipeline.sink['host'],
                    port=pipeline.sink['port'],
                    user=pipeline.sink['user'],
                    password=pipeline.sink['password'],
                    zone=pipeline.sink['zone'],
                    path=pipeline.sink['irods_path'])

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

            if input_kind == 'directory':
                dagster_pipeline = reconstructable(construct_pipeline_with_input_directory)(pipeline, pipeline.source['irods_path'])
                local_path = join(pipeline.workdir, 'input')
                os.mkdir(local_path)
                files = inputStore.list()
                print(f"Pulling input {input_kind} {files}")
                inputStore.pull(local_path)
            if input_kind == 'file':
                pass
            else:
                dagster_pipeline = construct_pipeline_with_no_input(pipeline)

            # TODO support for more queueing systems? HTCondor, what else?
            if not (self.name == "pbs" or self.name == "slurm"):
                raise ValueError(f"Queue type '{self.name}' not supported")

            print(f"Running {self.name} pipeline")

            for event in execute_pipeline_iterator(
                    dagster_pipeline,
                    run_config=run_config,
                    instance=DagsterInstance.get()):
                print(f"Dagster event '{event.event_type}' with message '{event.message}'")
                if event.event_type is DagsterEventType.PIPELINE_INIT_FAILURE or event.is_pipeline_failure:
                    raise PipelineException(event.message)

            print(f"Successfully ran {self.name} pipeline")

            if output_kind == 'file':
                local_path = pipeline.sink['local_path']
                print(f"Pushing output {output_kind} {local_path}")
                outputStore.push(local_path)
        except Exception:
            print(f"Failed to run {self.name} pipeline: {traceback.format_exc()}")
            return
