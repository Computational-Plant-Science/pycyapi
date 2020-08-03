import traceback

import dask
from dask.distributed import Client
from dagster import execute_pipeline_iterator, DagsterEventType

from plantit_cli.dagster.solids import *
from plantit_cli.exceptions import PlantitException
from plantit_cli.executor.executor import Executor
from plantit_cli.run import Run
from plantit_cli.store.irods import IRODSOptions


class InProcessExecutor(Executor):
    name = "in-process"

    def __init__(self, irods_options: IRODSOptions = None):
        if irods_options is not None:
            self.irods_options = irods_options

    def __run_config(self, run: Run):
        return {
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
            }
        }

    # def execute(self, run: Run):
    #     update_status(run, 3, f"Starting run '{run.identifier}' with '{self.name}' executor.")
    #     try:
    #         if run.clone is not None and run.clone is not '':
    #             self.clone_repo(run)

    #         if run.input:
    #             dagster_pipeline = self.pull_input(run)
    #         else:
    #             dagster_pipeline = construct_pipeline_with_no_input_for(run)

    #         update_status(run, 3, f"Running '{run.image}' container(s).")
    #         for event in execute_pipeline_iterator(dagster_pipeline, run_config=self.__run_config(run)):
    #             if event.event_type is DagsterEventType.PIPELINE_INIT_FAILURE or event.is_pipeline_failure:
    #                 raise PlantitException(event.message)

    #         if run.output:
    #             self.push_output(run)
    #     except Exception:
    #         update_status(run, 2, f"Run '{run.identifier}' failed: {traceback.format_exc()}")
    #         return

    def execute(self, run: Run):
        update_status(run, 3, f"Starting run '{run.identifier}' with '{self.name}' executor.")
        try:
            with Client() as client:
                if run.clone is not None and run.clone is not '':
                    self.clone_repo(run)

                if run.input:
                    input_directory = self.pull_input(run)
                    if run.input['kind'] == 'directory':
                        self.execute_workflow_with_directory_input(run, client, input_directory)
                    elif run.input['kind'] == 'file':
                        self.execute_workflow_with_file_input(run, client, input_directory)
                    else:
                        raise ValueError(f"Value of 'input.kind' must be either 'file' or 'directory'")
                else:
                    self.execute_workflow_with_no_input(run, client)

                if run.output:
                    self.push_output(run)
        except Exception:
            update_status(run, 2, f"Run '{run.identifier}' failed: {traceback.format_exc()}")
            return
