import os
import subprocess
from os.path import join

import requests

from plantit_cli.exceptions import PlantitException
from plantit_cli.run import Run


def update_status(run: Run, state: int, description: str):
    print(description)
    if run.api_url:
        requests.post(run.api_url,
                      data={
                          'run_id': run.identifier,
                          'state': state,
                          'description': description
                      },
                      headers={"Authorization": f"Token {run.plantit_token}"})


def __run_container(run: Run):
    cmd = f"singularity exec --home {run.workdir} {run.image} {run.command}"
    for param in sorted(run.params, key=lambda p: len(p['key']), reverse=True):
        cmd = cmd.replace(f"${param['key'].upper()}", param['value'])
    msg = f"Running '{run.image}' container with command: '{cmd}'"
    ret = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

    update_status(run, 3, msg)

    if ret.returncode != 0:
        msg = f"Non-zero exit code from container: {ret.stderr.decode('utf-8') if ret.stderr else ret.stdout.decode('utf-8') if ret.stdout else 'Unknown error'}"
        update_status(run, 2, msg)
        raise PlantitException(msg)
    else:
        msg = ret.stdout.decode('utf-8') + ret.stderr.decode('utf-8')
        update_status(run, 3, msg)
        msg = f"Successfully ran container with command: '{cmd}'"

    return msg


def run_container_no_input(run: Run):
    if run.params:
        params = run.params.copy()
    else:
        params = []

    if run.output:
        output_path = join(run.workdir, run.output['from']) if 'from' in run.output else run.workdir
        params += [{'key': 'OUTPUT', 'value': output_path}]

    update_status(run, 3, f"Running '{run.image}' container for '{run.identifier}'...")

    run = Run(
        identifier=run.identifier,
        plantit_token=run.plantit_token,
        api_url=run.api_url,
        workdir=run.workdir,
        image=run.image,
        command=run.command,
        params=params,
        input=run.input,
        output=run.output
    )
    result = __run_container(run)

    update_status(run, 3, result)


def run_container_directory_input(run: Run, input_directory: str):
    if run.params:
        params = run.params.copy()
    else:
        params = []

    params += [{'key': 'INPUT', 'value': input_directory}]

    if run.output:
        output_path = join(run.workdir, run.output['from']) if run.output['from'] is not '' else run.workdir
        params += [{'key': 'OUTPUT', 'value': output_path}]

    update_status(run, 3,
                  f"Running '{run.image}' container for '{run.identifier}' on input directory '{input_directory}'...")

    run = Run(
        identifier=run.identifier,
        plantit_token=run.plantit_token,
        api_url=run.api_url,
        workdir=run.workdir,
        image=run.image,
        command=run.command,
        params=params,
        input=run.input,
        output=run.output)
    result = __run_container(run)

    update_status(run, 3, result)


def run_container_file_input(run: Run, input_directory: str):
    files = os.listdir(input_directory)

    update_status(run, 3,
                  f"Running {len(files)} '{run.image}' container(s) for '{run.identifier}' on {len(files)} file(s) in input directory '{input_directory}'...")

    for file in files:
        if run.params:
            params = run.params.copy()
        else:
            params = []

        output = {}
        params += [{'key': 'INPUT', 'value': join(input_directory, file)}]

        if run.output:
            output = run.output.copy()
            params += [{'key': 'OUTPUT', 'value': join(run.workdir, output['from'])}]

        run = Run(
            identifier=run.identifier,
            plantit_token=run.plantit_token,
            api_url=run.api_url,
            workdir=run.workdir,
            image=run.image,
            command=run.command,
            params=params,
            input=run.input,
            output=output)
        result = __run_container(run)

        update_status(run, 3, result)
