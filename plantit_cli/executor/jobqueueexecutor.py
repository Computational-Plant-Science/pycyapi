import traceback

from dagster import execute_pipeline_iterator, DagsterInstance, DagsterEventType

from plantit_cli.dagster.solids import construct_pipeline_with_no_input, update_status
from plantit_cli.exceptions import PipelineException
from plantit_cli.executor.executor import Executor
from plantit_cli.run import Run
from plantit_cli.store.irodsstore import IRODSStore


def object_hook(dct):
    if 'path' in dct:
        print(dct)
        return IRODSStore(**dct)
    return dct


class JobQueueExecutor(Executor):
    """
    Runs workflows on an HPC/HTC queueing system with dask-jobqueue.
    """

    name = None

    def __init__(self, **kwargs):
        self.name = kwargs["name"]
        self.kwargs = kwargs

    def __run_config(self, run: Run):
        return {
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
                        'base_dir': run.workdir
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

    def execute(self, run: Run):
        """
        Runs a run on an HPC/HTC queueing system with dask-jobqueue.

        Args:
            run: The run definition.
        """

        update_status(run, 3, f"Starting '{run.identifier}' with '{self.name}' executor.")
        try:
            if run.clone is not None and run.clone is not '':
                Executor.clone(run)

            if run.input:
                dagster_pipeline = Executor.input(run)
            else:
                dagster_pipeline = construct_pipeline_with_no_input(run)

            run_config = self.__run_config(run)
            for k, v in self.kwargs.items():
                if k == "name":
                    continue
                run_config['execution']['dask']['config']['cluster'][self.name][k] = v

            # TODO support for more queueing systems? HTCondor, what else?
            if not (self.name == "pbs" or self.name == "slurm"):
                raise ValueError(f"Queue type '{self.name}' not supported")

            update_status(run, 3, f"Running '{run.image}' container(s) for '{run.identifier}'")
            for event in execute_pipeline_iterator(
                    dagster_pipeline,
                    run_config=run_config,
                    instance=DagsterInstance.get()):
                if event.event_type is DagsterEventType.PIPELINE_INIT_FAILURE or event.is_pipeline_failure:
                    raise PipelineException(event.message)
            print(f"Completed '{run.identifier}'.")
        except Exception:
            print(f"Failed to complete '{run.identifier}': {traceback.format_exc()}")
            return
