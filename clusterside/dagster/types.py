from dagster import input_hydration_config, Permissive, Int, Field, String, DagsterType, Dict, Selector, List

from clusterside.workflow import Workflow


class DaskJobqueueOptions:
    def __init__(self,
                 cores: int,
                 memory: str,
                 processes: int,
                 queue: str,
                 local_directory: str,
                 walltime: str):
        self.cores = cores
        self.memory = memory
        self.processes = processes
        self.queue = queue
        self.local_directory = local_directory
        self.walltime = walltime


@input_hydration_config(Permissive({
    'cores': Field(Int),
    'memory': Field(String),
    'processes': Field(Int),
    'queue': Field(String),
    'local_directory': Field(String),
    'walltime': Field(String)}
))
def dask_jobqueue_options_input_hydration_config(_, selector) -> DaskJobqueueOptions:
    return DaskJobqueueOptions(
        cores=selector['cores'],
        memory=selector['memory'],
        processes=selector['processes'],
        queue=selector['queue'],
        local_directory=selector['local_directory'],
        walltime=selector['walltime'])


DagsterDaskJobqueueOptions = DagsterType(
    name='DaskJobqueueOptions',
    type_check_fn=lambda _, value: isinstance(value, DaskJobqueueOptions),
    description='Dask-Jobqueue cluster queue_options',
    input_hydration_config=dask_jobqueue_options_input_hydration_config
)


@input_hydration_config(Permissive({
    'job_pk': Field(String),
    'token': Field(String),
    'server_url': Field(String),
    'container_url': Field(String),
    'commands': Field(String),
    'pre_commands': Field(String),
    'post_commands': Field(String),
    'flags': Field(List)}
))
def workflow_input_hydration_config(_, selector) -> Workflow:
    return Workflow(
        job_pk=selector['job_pk'],
        token=selector['token'],
        server_url=selector['server_url'],
        container_url=selector['container_url'],
        commands=selector['commands'],
        pre_commands=selector['pre_commands'],
        post_commands=selector['post_commands'],
        flags=selector['flags']
    )


DagsterWorkflow = DagsterType(
    name='Workflow',
    type_check_fn=lambda _, value: isinstance(value, Workflow),
    description='Workflow definition',
    input_hydration_config=workflow_input_hydration_config
)