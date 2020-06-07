from dagster import pipeline, ModeDefinition, default_executors
from dagster_dask import dask_executor

from plantit_cluster.dagster.solids import *


@pipeline(mode_defs=[ModeDefinition(executor_defs=default_executors + [dask_executor])])
def singularity():
    targets(singularity_container(sources()))
