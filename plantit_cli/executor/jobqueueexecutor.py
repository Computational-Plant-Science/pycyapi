import traceback

from dagster import execute_pipeline_iterator, DagsterInstance, DagsterEventType

from plantit_cli.dagster.solids import construct_pipeline_with_no_input, update_status
from plantit_cli.exceptions import PlantitException
from plantit_cli.executor.executor import Executor
from plantit_cli.run import Run


class JobQueueExecutor(Executor):

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
        update_status(run, 3, f"Starting '{run.identifier}' with '{self.name}' executor.")
        try:
            # TODO support for more queueing systems? HTCondor, what else?
            if not (self.name == "pbs" or self.name == "slurm"):
                raise ValueError(f"Queue type '{self.name}' not supported")

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

            update_status(run, 3, f"Running '{run.image}' container(s) for '{run.identifier}'")
            for event in execute_pipeline_iterator(
                    dagster_pipeline,
                    run_config=run_config,
                    instance=DagsterInstance.get()):
                if event.event_type is DagsterEventType.PIPELINE_INIT_FAILURE or event.is_pipeline_failure:
                    raise PlantitException(event.message)

            if run.output:
                Executor.output(run)
        except Exception:
            update_status(run, 2, f"Failed to complete '{run.identifier}': {traceback.format_exc()}")
            return
