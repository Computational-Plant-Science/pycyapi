from dagster import input_hydration_config, Permissive, Int, Field, String, DagsterType, Dict, Selector, List, Array

from plantit_cluster.job import Job


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
    'id': Field(String),
    'token': Field(String),
    'workdir': Field(String),
    'container': Field(String),
    'commands': Field(String)}))
def job_input_hydration_config(_, selector) -> Job:
    return Job(
        id=selector['id'],
        token=selector['token'],
        workdir=selector['workdir'],
        server=selector['server'] if 'server' in selector else None,
        container=selector['container'],
        commands=str(selector['commands']).split())


DagsterJob = DagsterType(
    name='Job',
    type_check_fn=lambda _, value: isinstance(value, Job),
    description='Job definition',
    input_hydration_config=job_input_hydration_config
)