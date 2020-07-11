import os
import traceback
from os.path import join

from dagster import execute_pipeline_iterator, DagsterEventType

from plantit_cluster.dagster.solids import *
from plantit_cluster.exceptions import PipelineException
from plantit_cluster.executor.executor import Executor
from plantit_cluster.store.irodsstore import IRODSStore
from plantit_cluster.run import Run


class InProcessExecutor(Executor):
    """
    Runs pipelines in-process.
    """

    name = "in-process"

    def execute(self, pipeline: Run):
        """
        Runs a pipeline in-process.

        Args:
            pipeline: The pipeline definition.
        """

        try:
            if pipeline.source is not None:
                irods = IRODSStore(
                    path=pipeline.source.path,
                    host=pipeline.source.host,
                    port=pipeline.source.port,
                    user=pipeline.source.user,
                    password=pipeline.source.password,
                    zone=pipeline.source.zone,
                )
                print(f"Pulling input files")
                directory = join(pipeline.workdir, 'input')
                os.makedirs(directory, exist_ok=True)
                irods.pull(directory)
                files = [os.path.abspath(join(directory, file)) for file in os.listdir(directory)]
                print(f"Successfully pulled input files {files}")

                if pipeline.source.param is None:
                    dagster_pipeline = construct_pipeline_with_no_input(pipeline)
                elif pipeline.source.param == 'directory':
                    dagster_pipeline = construct_pipeline_with_input_directory(pipeline, directory)
                elif pipeline.source.param == 'file':
                    dagster_pipeline = construct_pipeline_with_input_files(pipeline, files)
                else:
                    raise ValueError(f"Value of 'input.param' must be either 'file' or 'directory'")
            else:
                dagster_pipeline = construct_pipeline_with_no_input(pipeline)

            print(f"Running {self.name} pipeline")
            for event in execute_pipeline_iterator(
                    dagster_pipeline,
                    run_config={
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
                        }
                    }):
                print(f"Dagster event '{event.event_type}' with message '{event.message}'")
                if event.event_type is DagsterEventType.PIPELINE_INIT_FAILURE or event.is_pipeline_failure:
                    raise PipelineException(event.message)
            print(f"Successfully ran {self.name} pipeline")
        except Exception:
            print(f"Failed to run {self.name} pipeline: {traceback.format_exc()}")
            return