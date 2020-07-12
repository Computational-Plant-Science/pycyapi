import os
import subprocess
import traceback
from os.path import join

from dagster import execute_pipeline_iterator, reconstructable, DagsterInstance, DagsterEventType

from plantit_cli.dagster.solids import construct_pipeline_with_input_directory, \
    construct_pipeline_with_no_input, update_status
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

            input_kind = None if run.input is None else run.input['kind']
            output_kind = None if run.output is None else run.output['kind']

            if input_kind is not None:
                inputStore = IRODSStore(
                    host=run.input['host'],
                    port=run.input['port'],
                    user=run.input['user'],
                    password=run.input['password'],
                    zone=run.input['zone'],
                    path=run.input['irods_path'])

            if output_kind is not None:
                outputStore = IRODSStore(
                    host=run.output['host'],
                    port=run.output['port'],
                    user=run.output['user'],
                    password=run.output['password'],
                    zone=run.output['zone'],
                    path=run.output['irods_path'])

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

            if input_kind == 'directory':
                dagster_pipeline = reconstructable(construct_pipeline_with_input_directory)(run, run.input['irods_path'])
                local_path = join(run.workdir, 'input')
                os.mkdir(local_path)
                files = inputStore.list()
                print(f"Pulling input {input_kind} {files}")
                inputStore.pull(local_path)
            if input_kind == 'file':
                pass
            else:
                dagster_pipeline = construct_pipeline_with_no_input(run)

            # TODO support for more queueing systems? HTCondor, what else?
            if not (self.name == "pbs" or self.name == "slurm"):
                raise ValueError(f"Queue type '{self.name}' not supported")

            print(f"Running {self.name} workflow")

            for event in execute_pipeline_iterator(
                    dagster_pipeline,
                    run_config=run_config,
                    instance=DagsterInstance.get()):
                print(f"Dagster event '{event.event_type}' with message '{event.message}'")
                if event.event_type is DagsterEventType.PIPELINE_INIT_FAILURE or event.is_pipeline_failure:
                    raise PipelineException(event.message)

            print(f"Successfully ran {self.name} workflow")

            if output_kind == 'file':
                local_path = run.output['local_path']
                print(f"Pushing output {output_kind} {local_path}")
                outputStore.push(local_path)
        except Exception:
            print(f"Failed to run {self.name} workflow: {traceback.format_exc()}")
            return
