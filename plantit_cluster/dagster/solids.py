import subprocess

import dagster
from dagster import solid, OutputDefinition, Output, default_executors, ModeDefinition
from dagster.core.definitions.composition import InvokedSolidOutputHandle
from dagster_dask import dask_executor

from plantit_cluster.exceptions import PipelineException
from plantit_cluster.run import Run


@solid
def run_container(context, run: Run):
    cmd = f"singularity exec --containall --home {run.workdir} {run.image} {' '.join(run.command)}"
    for param in run.params:
        cmd = cmd.replace(f"${param['key']}", param['value'])
    context.log.info(f"Running container with '{cmd}'")
    ret = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    if ret.returncode != 0:
        msg = f"Non-zero exit code from container: {ret.stderr.decode('utf-8') if ret.stderr else ret.stdout.decode('utf-8') if ret.stdout else 'Unknown error'}"
        context.log.error(msg)
        raise PipelineException(msg)
    else:
        context.log.info(
            f"Successfully ran container with output: {ret.stdout.decode('utf-8')}")


def construct_pipeline_with_no_input(run: Run):
    @solid(output_defs=[OutputDefinition(Run, 'run', is_required=True)])
    def yield_definition(context):
        yield Output(Run(
            workdir=run.workdir,
            image=run.image,
            command=run.command,
            params=run.params,
            source=run.source
        ), 'run')

    @dagster.pipeline(name='pipeline_with_no_inputs',
                      mode_defs=[ModeDefinition(executor_defs=default_executors + [dask_executor])])
    def construct():
        run_container(yield_definition())

    return construct


def construct_pipeline_with_input_directory(run: Run, directory: str):
    @solid(output_defs=[OutputDefinition(Run, 'run', is_required=True)])
    def yield_definition(context):
        yield Output(Run(
            workdir=run.workdir,
            image=run.image,
            command=run.command,
            params=run.params + [f"INPUT={directory}"],
            source=run.source
        ), 'run')

    @dagster.pipeline(name='pipeline_with_input_directory',
                      mode_defs=[ModeDefinition(executor_defs=default_executors + [dask_executor])])
    def construct():
        run_container(yield_definition())

    return construct


def construct_pipeline_with_input_files(run: Run, files: [str] = []):
    output_defs = [
        OutputDefinition(Run, name.split('/')[-1], is_required=False) for name in files
    ]

    @solid(output_defs=output_defs)
    def yield_definitions(context):
        for name in files:
            yield Output(Run(
                workdir=run.workdir,
                image=run.image,
                command=run.command,
                params=run.params + [f"INPUT={name}"],
                source=run.source
            ), name.split('/')[-1])

    @solid(output_defs=[OutputDefinition(Run, 'run', is_required=True)])
    def yield_definition(context):
        yield Output(Run(
            workdir=run.workdir,
            image=run.image,
            command=run.command,
            params=run.params,
            source=run.source
        ), 'run')

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
