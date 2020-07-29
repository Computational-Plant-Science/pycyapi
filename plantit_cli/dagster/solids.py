import os
import subprocess
from os.path import join

import dagster
import requests
from dagster import solid, OutputDefinition, Output, default_executors, ModeDefinition
from dagster.core.definitions.composition import InvokedSolidOutputHandle
from dagster_dask import dask_executor

from plantit_cli.exceptions import PlantitException
from plantit_cli.run import Run


def update_status(run: Run, state: int, description: str):
    print(description)
    status = {
        'run_id': run.identifier,
        'state': state,
        'description': description
    }
    if run.api_url:
        requests.post(run.api_url,
                      data=status,
                      headers={"Authorization": f"Token {run.token}"})


@solid
def run_container(context, run: Run):
    cmd = f"singularity exec --home {run.workdir} {run.image} {run.command}"
    for param in sorted(run.params, key=lambda p: len(p['key']), reverse=True):
        cmd = cmd.replace(f"${param['key'].upper()}", param['value'])
    msg = f"Running container with command: '{cmd}'"
    update_status(run, 3, msg)
    ret = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    if ret.returncode != 0:
        msg = f"Non-zero exit code from container: {ret.stderr.decode('utf-8') if ret.stderr else ret.stdout.decode('utf-8') if ret.stdout else 'Unknown error'}"
        update_status(run, 2, msg)
        raise PlantitException(msg)
    else:
        msg = ret.stdout.decode('utf-8') + ret.stderr.decode('utf-8')
        update_status(run, 3, msg)
        msg = "Successfully ran container."
        update_status(run, 3, msg)


def construct_pipeline_with_no_input_for(run: Run):
    @solid(output_defs=[OutputDefinition(Run, 'container', is_required=True)])
    def yield_definition(context):
        if run.params:
            params = run.params.copy()
        else:
            params = []

        if run.output:
            output_path = join(run.workdir, run.output['local_path']) if run.output['local_path'] is not '' else run.workdir
            params += [{'key': 'OUTPUT', 'value': output_path}]

        yield Output(Run(
            identifier=run.identifier,
            token=run.token,
            api_url=run.api_url,
            workdir=run.workdir,
            image=run.image,
            command=run.command,
            params=params,
            input=run.input,
            output=run.output
        ), 'container')

    @dagster.pipeline(name='workflow_with_no_input',
                      mode_defs=[ModeDefinition(executor_defs=default_executors + [dask_executor])])
    def construct():
        run_container(yield_definition())

    return construct


def construct_pipeline_with_input_directory(run: Run, directory: str):
    @solid(output_defs=[OutputDefinition(Run, f"container_for_{just_name(directory)}", is_required=True)])
    def yield_definition(context):
        if run.params:
            params = run.params.copy()
        else:
            params = []

        if run.input:
            params += [{'key': 'INPUT', 'value': directory}]

        if run.output:
            output_path = join(run.workdir, run.output['local_path']) if run.output['local_path'] is not '' else run.workdir
            params += [{'key': 'OUTPUT', 'value': output_path}]

        yield Output(Run(
            identifier=run.identifier,
            token=run.token,
            api_url=run.api_url,
            workdir=run.workdir,
            image=run.image,
            command=run.command,
            params=params,
            input=run.input,
            output=run.output
        ), f"container_for_{just_name(directory)}")

    @dagster.pipeline(name='workflow_with_directory_input',
                      mode_defs=[ModeDefinition(executor_defs=default_executors + [dask_executor])])
    def construct():
        run_container(yield_definition())

    return construct


def just_name(path):
    return os.path.splitext(path.split('/')[-1])[0]


def construct_pipeline_with_input_files(run: Run, files: [str] = []):
    output_defs = [
        OutputDefinition(Run, f"container_for_{just_name(file)}", is_required=False) for file in files
    ]

    @solid(output_defs=output_defs)
    def yield_definitions(context):
        for file in files:
            if run.params:
                params = run.params.copy()
            else:
                params = []

            if run.input:
                params += [{'key': 'INPUT', 'value': file}]

            output = {}
            if output:
                output = run.output.copy()
                params += [{'key': 'OUTPUT', 'value': join(run.workdir, output['local_path'])}]

            yield Output(Run(
                identifier=run.identifier,
                token=run.token,
                api_url=run.api_url,
                workdir=run.workdir,
                image=run.image,
                command=run.command,
                params=params,
                input=run.input,
                output=output
            ), f"container_for_{just_name(file)}")

    @solid(output_defs=[OutputDefinition(Run, 'container', is_required=True)])
    def yield_definition(context):
        yield Output(run, 'container')

    @dagster.pipeline(name='workflow_with_file_input',
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
