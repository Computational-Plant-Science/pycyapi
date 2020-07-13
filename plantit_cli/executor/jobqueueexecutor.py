import os
import subprocess
import traceback
from os.path import join

from dagster import execute_pipeline_iterator, reconstructable, DagsterInstance, DagsterEventType

from plantit_cli.dagster.solids import construct_pipeline_with_input_directory, \
    construct_pipeline_with_no_input, update_status, construct_pipeline_with_input_files
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

    def execute(self, run: Run):
        """
        Runs a pipeline on an HPC/HTC queueing system with dask-jobqueue.

        Args:
            run: The run definition.
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

            for k, v in self.kwargs.items():
                if k == "name":
                    continue
                run_config['execution']['dask']['config']['cluster'][self.name][k] = v

            # TODO support for more queueing systems? HTCondor, what else?
            if not (self.name == "pbs" or self.name == "slurm"):
                raise ValueError(f"Queue type '{self.name}' not supported")

            print(f"Running {self.name} workflow")

            for event in execute_pipeline_iterator(
                    dagster_pipeline,
                    run_config=run_config,
                    instance=DagsterInstance.get()):
                if event.event_type is DagsterEventType.PIPELINE_INIT_FAILURE or event.is_pipeline_failure:
                    raise PipelineException(event.message)
            print(f"Successfully completed '{run.identifier}'.")
        except Exception:
            print(f"Failed to complete '{run.identifier}': {traceback.format_exc()}")
            return
