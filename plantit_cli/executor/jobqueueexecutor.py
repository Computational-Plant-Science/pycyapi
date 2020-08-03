import traceback

from dagster import execute_pipeline_iterator, DagsterInstance, DagsterEventType
from dask.distributed import Client

from plantit_cli.dagster.solids import construct_pipeline_with_no_input_for, update_status
from plantit_cli.exceptions import PlantitException
from plantit_cli.executor.executor import Executor
from plantit_cli.run import Run
from plantit_cli.store.irods import IRODSOptions


class JobQueueExecutor(Executor):

    name = None

    def __init__(self, name, irods_options: IRODSOptions = None, **kwargs):
        if irods_options is not None:
            self.irods_options = irods_options
        self.name = name
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

    # def execute(self, run: Run):
    #     update_status(run, 3, f"Starting '{run.identifier}' with '{self.name}' executor.")
    #     try:
    #         # TODO support for more queueing systems? HTCondor, what else?
    #         if not (self.name == "pbs" or self.name == "slurm"):
    #             raise ValueError(f"Queue type '{self.name}' not supported")

    #         if run.clone is not None and run.clone is not '':
    #             self.clone_repo(run)

    #         if run.input:
    #             dagster_pipeline = self.pull_input(run)
    #         else:
    #             dagster_pipeline = construct_pipeline_with_no_input_for(run)

    #         run_config = self.__run_config(run)
    #         for k, v in self.kwargs.items():
    #             if k == "name":
    #                 continue
    #             run_config['execution']['dask']['config']['cluster'][self.name][k] = v

    #         update_status(run, 3, f"Running '{run.image}' container(s).")
    #         for event in execute_pipeline_iterator(
    #                 dagster_pipeline,
    #                 run_config=run_config,
    #                 instance=DagsterInstance.get()):
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
            # TODO support for more queueing systems? HTCondor, what else?
            if self.name == 'pbs':
                from dask_jobqueue import PBSCluster
                cluster = PBSCluster(**self.kwargs)
            elif self.name == 'slurm':
                from dask_jobqueue import SLURMCluster
                cluster = SLURMCluster(**self.kwargs)
            else:
                raise ValueError(f"Queue type '{self.name}' not supported")

            with Client(cluster) as client:
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
