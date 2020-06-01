from dagster import pipeline, ModeDefinition, default_executors
from dagster_dask import dask_executor

from clusterside.dagster.solids import *


@pipeline(mode_defs=ModeDefinition(executor_defs=default_executors + [dask_executor]))
def singularity_workflow():
    load_outputs(run_post_commands(run_container(run_pre_commands(extract_inputs()))))
