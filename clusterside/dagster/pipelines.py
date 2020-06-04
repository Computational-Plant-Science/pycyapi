from dagster import pipeline, ModeDefinition, default_executors
from dagster_dask import dask_executor

from clusterside.dagster.solids import *


@pipeline(mode_defs=[ModeDefinition(executor_defs=default_executors + [dask_executor])])
def singularity_job():
    targets(container(sources()))
