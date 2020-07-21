import traceback

from dagster import execute_pipeline_iterator, DagsterInstance, DagsterEventType

from plantit_cli.dagster.solids import construct_pipeline_with_no_input_for, update_status
from plantit_cli.exceptions import PlantitException
from plantit_cli.executor.executor import Executor
from plantit_cli.run import Run
from plantit_cli.store.irods import IRODSOptions


class JobQueueExecutor(Executor):

    name = None

    def __init__(self, irods_options: IRODSOptions = None, **kwargs):
        if irods_options is not None:
            self.irods_options = irods_options
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
                self.clone_repo_for(run)

            if run.input:
                dagster_pipeline = self.pull_input_and_construct_pipeline(run)
            else:
                dagster_pipeline = construct_pipeline_with_no_input_for(run)

            run_config = self.__run_config(run)
            for k, v in self.kwargs.items():
                if k == "name":
                    continue
                run_config['execution']['dask']['config']['cluster'][self.name][k] = v

            update_status(run, 3, f"Running '{run.image}' container(s).")
            for event in execute_pipeline_iterator(
                    dagster_pipeline,
                    run_config=run_config,
                    instance=DagsterInstance.get()):
                if event.event_type is DagsterEventType.PIPELINE_INIT_FAILURE or event.is_pipeline_failure:
                    raise PlantitException(event.message)

            if run.output:
                self.push_output(run)
        except Exception:
            update_status(run, 2, f"Run '{run.identifier}' failed: {traceback.format_exc()}")
            return
