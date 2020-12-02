import os
import copy
import subprocess
from os.path import join

import requests

from plantit_cli.exceptions import PlantitException
from plantit_cli.plan import Plan


def update_status(run: Plan, state: int, description: str):
    print(description)
    if run.api_url:
        requests.post(run.api_url,
                      data={
                          'run_id': run.identifier,
                          'state': state,
                          'description': description
                      },
                      headers={"Authorization": f"Token {run.plantit_token}"})


def __run_container(plan: Plan):
    cmd = f"singularity exec --home {plan.workdir} {plan.image} {plan.command}"
    for param in sorted(plan.params, key=lambda p: len(p['key']), reverse=True):
        cmd = cmd.replace(f"${param['key'].upper()}", param['value'])
    msg = f"Running '{plan.image}' container with command: '{cmd}'"
    ret = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    update_status(plan, 3, msg)

    if ret.returncode != 0:
        msg = f"Non-zero exit code from container: {ret.stderr.decode('utf-8') if ret.stderr else ret.stdout.decode('utf-8') if ret.stdout else 'Unknown error'}"
        update_status(plan, 2, msg)
        raise PlantitException(msg)
    else:
        msg = ret.stdout.decode('utf-8') + ret.stderr.decode('utf-8')
        update_status(plan, 3, msg)
        msg = f"Successfully ran container with command: '{cmd}'"
    return msg


def run_container(plan: Plan):
    params = plan.params.copy() if plan.params else []
    if plan.output:
        output_path = join(plan.workdir, plan.output['from']) if 'from' in plan.output else plan.workdir
        params += [{'key': 'OUTPUT', 'value': output_path}]
    update_status(plan, 3, f"Running '{plan.image}' container for '{plan.identifier}'")
    update_status(plan, 3, __run_container(Plan(
        identifier=plan.identifier,
        plantit_token=plan.plantit_token,
        api_url=plan.api_url,
        workdir=plan.workdir,
        image=plan.image,
        command=plan.command,
        params=params,
        input=plan.input,
        output=plan.output
    )))


def run_container_for_directory(plan: Plan, input_directory: str):
    params = (plan.params.copy() if plan.params else []) + [{'key': 'INPUT', 'value': input_directory}]
    if plan.output:
        output_path = join(plan.workdir, plan.output['from']) if plan.output['from'] != '' else plan.workdir
        params += [{'key': 'OUTPUT', 'value': output_path}]
    update_status(plan, 3,
                  f"Running '{plan.image}' container for '{plan.identifier}' on input directory '{input_directory}'")
    update_status(plan, 3, __run_container(Plan(
        identifier=plan.identifier,
        plantit_token=plan.plantit_token,
        api_url=plan.api_url,
        workdir=plan.workdir,
        image=plan.image,
        command=plan.command,
        params=params,
        input=plan.input,
        output=plan.output)))


def run_containers_for_files(run: Plan, input_directory: str):
    files = os.listdir(input_directory)
    update_status(run, 3,
                  f"Running {len(files)} '{run.image}' container(s) for '{run.identifier}' on {len(files)} file(s) in input directory '{input_directory}'")
    for file in files:
        params = (copy.deepcopy(run.params) if run.params else []) + [
            {'key': 'INPUT', 'value': join(input_directory, file)}]
        output = {}
        if run.output:
            output = copy.deepcopy(run.output)
            params += [{'key': 'OUTPUT', 'value': join(run.workdir, output['from'])}]
        update_status(run, 3, __run_container(Plan(
            identifier=run.identifier,
            plantit_token=run.plantit_token,
            api_url=run.api_url,
            workdir=run.workdir,
            image=run.image,
            command=run.command,
            params=params,
            input=run.input,
            output=output)))
