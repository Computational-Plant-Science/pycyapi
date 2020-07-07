import os
import subprocess
from os.path import join

import dagster
from dagster import solid, OutputDefinition, Output, default_executors, ModeDefinition
from dagster.core.definitions.composition import InvokedSolidOutputHandle
from dagster_dask import dask_executor

from plantit_cluster.exceptions import PlantitException
from plantit_cluster.pipeline import Pipeline


@solid
def run_container(context, pipeline: Pipeline):
    cmd = f"singularity exec --containall --home {pipeline.workdir} {pipeline.image} {' '.join(pipeline.commands)}"
    for param in pipeline.params:
        split = param.split('=')
        cmd = cmd.replace(f"${split[0]}", split[1])
    context.log.info(f"Running container with '{cmd}'")
    ret = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    if ret.returncode != 0:
        msg = f"Non-zero exit code from container: {ret.stderr.decode('utf-8') if ret.stderr else ret.stdout.decode('utf-8') if ret.stdout else 'Unknown error'}"
        context.log.error(msg)
        raise PlantitException(msg)
    else:
        context.log.info(
            f"Successfully ran container with output: {ret.stdout.decode('utf-8')}")


def construct_pipeline_with_no_input(pipeline: Pipeline):
    @solid(output_defs=[OutputDefinition(Pipeline, 'pipeline', is_required=True)])
    def yield_definition(context):
        yield Output(Pipeline(
            workdir=pipeline.workdir,
            image=pipeline.image,
            commands=pipeline.commands,
            params=pipeline.params,
            input=pipeline.input
        ), 'pipeline')

    @dagster.pipeline(name='pipeline_with_no_inputs',
                      mode_defs=[ModeDefinition(executor_defs=default_executors + [dask_executor])])
    def construct():
        run_container(yield_definition())

    return construct


def construct_pipeline_with_input_directory(pipeline: Pipeline, directory: str):
    @solid(output_defs=[OutputDefinition(Pipeline, 'pipeline', is_required=True)])
    def yield_definition(context):
        yield Output(Pipeline(
            workdir=pipeline.workdir,
            image=pipeline.image,
            commands=pipeline.commands,
            params=pipeline.params + [f"INPUT={directory}"],
            input=pipeline.input
        ), 'pipeline')

    @dagster.pipeline(name='pipeline_with_input_directory',
                      mode_defs=[ModeDefinition(executor_defs=default_executors + [dask_executor])])
    def construct():
        run_container(yield_definition())

    return construct


def construct_pipeline_with_input_files(pipeline: Pipeline, files: [str] = []):
    output_defs = [
        OutputDefinition(Pipeline, name.split('/')[-1], is_required=False) for name in files
    ]

    @solid(output_defs=output_defs)
    def yield_definitions(context):
        for name in files:
            yield Output(Pipeline(
                workdir=pipeline.workdir,
                image=pipeline.image,
                commands=pipeline.commands,
                params=pipeline.params + [f"INPUT={name}"],
                input=pipeline.input
            ), name.split('/')[-1])

    @solid(output_defs=[OutputDefinition(Pipeline, 'pipeline', is_required=True)])
    def yield_definition(context):
        yield Output(Pipeline(
            workdir=pipeline.workdir,
            image=pipeline.image,
            commands=pipeline.commands,
            params=pipeline.params,
            input=pipeline.input
        ), 'pipeline')

    @dagster.pipeline(name='pipeline_with_input_files',
                      mode_defs=[ModeDefinition(executor_defs=default_executors + [dask_executor])])
    def construct():
        definitions = yield_definitions()
        if definitions is None:
            run_container(yield_definition())
        elif type(definitions) is InvokedSolidOutputHandle:
            run_container(definitions)
        else:
            for definition in definitions:
                run_container.alias(definition.output_name)(definition)

    return construct
