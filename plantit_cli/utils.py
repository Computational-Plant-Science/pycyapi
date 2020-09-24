import os
import subprocess
from os.path import join

import requests

import dask
from dask.distributed import Client

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
                      headers={"Authorization": f"Token {run.plantit_token}"})


def execute_container(run: Run):
    cmd = f"singularity exec --home {run.workdir} {run.image} {run.command}"
    for param in sorted(run.params, key=lambda p: len(p['key']), reverse=True):
        cmd = cmd.replace(f"${param['key'].upper()}", param['value'])
    msg = f"Running '{run.image}' container with command: '{cmd}'"
    update_status(run, 3, msg)
    ret = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    if ret.returncode != 0:
        msg = f"Non-zero exit code from container: {ret.stderr.decode('utf-8') if ret.stderr else ret.stdout.decode('utf-8') if ret.stdout else 'Unknown error'}"
        update_status(run, 2, msg)
        raise PlantitException(msg)
    else:
        msg = ret.stdout.decode('utf-8') + ret.stderr.decode('utf-8')
        update_status(run, 3, msg)
        msg = f"Successfully ran container with command: '{cmd}'"
    return msg


def execute_workflow_with_no_input(run: Run, client: Client):
    if run.params:
        params = run.params.copy()
    else:
        params = []

    if run.output:
        output_path = join(run.workdir, run.output['from']) if run.output[
                                                                         'from'] is not '' else run.workdir
        params += [{'key': 'OUTPUT', 'value': output_path}]

    future = client.submit(execute_container, Run(
        identifier=run.identifier,
        plantit_token=run.plantit_token,
        api_url=run.api_url,
        workdir=run.workdir,
        image=run.image,
        command=run.command,
        params=params,
        input=run.input,
        output=run.output
    ))

    update_status(run, 2, future.result())


def execute_workflow_with_directory_input(run: Run, client: Client, input_directory: str):
    if run.params:
        params = run.params.copy()
    else:
        params = []

    params += [{'key': 'INPUT', 'value': input_directory}]

    if run.output:
        output_path = join(run.workdir, run.output['from']) if run.output[
                                                                         'from'] is not '' else run.workdir
        params += [{'key': 'OUTPUT', 'value': output_path}]

    update_status(run, 3,
                  f"Running '{run.image}' container on input directory '{input_directory}'.")

    future = client.submit(execute_container, Run(
        identifier=run.identifier,
        plantit_token=run.plantit_token,
        api_url=run.api_url,
        workdir=run.workdir,
        image=run.image,
        command=run.command,
        params=params,
        input=run.input,
        output=run.output
    ))

    update_status(run, 2, future.result())


def execute_workflow_with_file_input(run: Run, client: Client, input_directory: str):
    files = os.listdir(input_directory)
    futures = []

    update_status(run, 3,
                  f"Running {len(files)} '{run.image}' container(s) on {len(files)} files in input directory '{input_directory}'.")

    for file in files:
        if run.params:
            params = run.params.copy()
        else:
            params = []

        params += [{'key': 'INPUT', 'value': join(input_directory, file)}]

        output = {}
        if run.output:
            output = run.output.copy()
            params += [{'key': 'OUTPUT', 'value': join(run.workdir, output['from'])}]

        future = client.submit(execute_container, Run(
            identifier=run.identifier,
            plantit_token=run.plantit_token,
            api_url=run.api_url,
            workdir=run.workdir,
            image=run.image,
            command=run.command,
            params=params,
            input=run.input,
            output=output
        ))

        futures.append(future)

    for future in dask.distributed.as_completed(futures):
        update_status(run, 2, future.result())
