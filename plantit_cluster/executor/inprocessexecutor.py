import traceback

from dagster import execute_pipeline_iterator, DagsterEventType

from plantit_cluster.dagster.solids import construct_pipeline
from plantit_cluster.exceptions import PlantitException
from plantit_cluster.executor.executor import Executor
from plantit_cluster.input.irodsinput import IRODSInput
from plantit_cluster.pipeline import Pipeline


class InProcessExecutor(Executor):
    """
    Runs pipelines in-process.
    """

    name = "in-process"

    def execute(self, pipeline: Pipeline):
        """
        Runs a pipeline in-process.

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


            print(f"Running {self.name} pipeline")
            for event in execute_pipeline_iterator(
                    # plantit_pipeline,
                    construct_pipeline(pipeline, files),
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
                    raise PlantitException(event.message)
            print(f"Successfully ran {self.name} pipeline")
        except Exception:
            print(f"Failed to run {self.name} pipeline: {traceback.format_exc()}")
            return