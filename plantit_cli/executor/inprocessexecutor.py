import os
import traceback
from os.path import join

from dagster import execute_pipeline_iterator, DagsterEventType

from plantit_cli.dagster.solids import *
from plantit_cli.exceptions import PipelineException
from plantit_cli.executor.executor import Executor
from plantit_cli.store.irodsstore import IRODSStore
from plantit_cli.run import Run


class InProcessExecutor(Executor):
    """
    Runs workflows in-process.
    """

    name = "in-process"

    def execute(self, run: Run):
        """
        Runs a workflow in-process.

        Args:

            run: The workflow definition.
        """

        try:
            if run.clone is not None and run.clone is not '':
                ret = subprocess.run(f"git clone {run.clone}", stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
                if ret.returncode != 0:
                    msg = f"Failed to clone repository '{run.clone}'"
                    print(msg)
                    update_status(run, 2, msg)
                    raise PipelineException(msg)
                else:
                    msg = f"Cloned repository '{run.clone}'"
                    print(msg)
                    update_status(run, 3, msg)

            if run.input:
                irods = IRODSStore(
                    path=run.input['irods_path'],
                    host=run.input['host'],
                    port=run.input['port'],
                    user=run.input['username'],
                    password=run.input['password'],
                    zone=run.input['zone'],
                )

                print(f"Pulling input (s) from '{run.input['irods_path']}'.")
                directory = join(run.workdir, 'input')
                os.makedirs(directory, exist_ok=True)
                irods.pull(directory)
                files = [os.path.abspath(join(directory, file)) for file in os.listdir(directory)]
                print(f"Successfully pulled input(s) {files}.")

                if run.input['kind'] == 'file':
                    dagster_pipeline = construct_pipeline_with_input_files(run, files)
                elif run.input['kind'] == 'directory':
                    dagster_pipeline = construct_pipeline_with_input_directory(run, directory)
                else:
                    raise ValueError(f"Value of 'input.kind' must be either 'file' or 'directory'")
            else:
                dagster_pipeline = construct_pipeline_with_no_input(run)

            print(f"Running '{run.identifier}' with '{self.name}' executor.")
            for event in execute_pipeline_iterator(
                    dagster_pipeline,
                    run_config={
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
                    }):
                if event.event_type is DagsterEventType.PIPELINE_INIT_FAILURE or event.is_pipeline_failure:
                    raise PipelineException(event.message)
            print(f"Successfully completed '{run.identifier}'.")
        except Exception:
            print(f"Failed to complete '{run.identifier}': {traceback.format_exc()}")
            return