import os
import copy
import subprocess
from os import listdir
from os.path import join, isfile

import requests

from plantit_cli.exceptions import PlantitException
from plantit_cli.plan import Plan


def list_files(path, include_pattern=None, include=None, exclude_pattern=None, exclude=None):
    all_paths = [join(path, file) for file in listdir(path) if isfile(join(path, file))]
    included_by_pattern = [p for p in all_paths if include_pattern.lower() in p.lower()] if include_pattern is not None else all_paths
    print(f"Included: {included_by_pattern}")
    included_by_name = [p for p in all_paths if
                     p.split('/')[-1] in [pi for pi in include]] if include is not None else included_by_pattern
    included = set(included_by_pattern + included_by_name)
    print(f"Included: {included}")
    excluded_by_pattern = [p for p in included if exclude_pattern.lower() not in p.lower()] if exclude_pattern is not None else included
    print(f"Included: {excluded_by_pattern}")
    excluded = [p for p in excluded_by_pattern if p.split('/')[-1] not in exclude] if exclude is not None else excluded_by_pattern
    print(f"Included: {excluded}")

    return excluded


def update_status(plan: Plan, state: int, description: str):
    print(description)
    if plan.api_url:
        requests.post(plan.api_url,
                      data={
                          'run_id': plan.identifier,
                          'state': state,
                          'description': description
                      },
                      headers={"Authorization": f"Token {plan.plantit_token}"})


def __run_container(plan: Plan):
    cmd = f"singularity exec --home {plan.workdir}{' --bind ' + plan.workdir + ':' + plan.mount if plan.mount is not None and plan.mount != '' else ''} {plan.image} {plan.command}"
    for param in sorted(plan.params, key=lambda p: len(p['key']), reverse=True):
        cmd = cmd.replace(f"${param['key'].upper()}", param['value'])
    msg = f"Running '{cmd}'"
    update_status(plan, 3, msg)

    with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, bufsize=1) as proc:
        for line in proc.stdout:
            update_status(plan, 3, line.decode('utf-8'))

    if proc.returncode:
        msg = f"Non-zero exit code from container"
        update_status(plan, 2, msg)
        raise PlantitException(msg)
    else:
        msg = f"Successfully ran container"

    # ret = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    # if ret.returncode != 0:
    #     msg = f"Non-zero exit code from container: {ret.stderr.decode('utf-8') if ret.stderr else ret.stdout.decode('utf-8') if ret.stdout else 'Unknown error'}"
    #     update_status(plan, 2, msg)
    #     raise PlantitException(msg)
    # else:
    #     msg = ret.stdout.decode('utf-8') + ret.stderr.decode('utf-8')
    #     update_status(plan, 3, msg)
    #     msg = f"Successfully ran container with command: '{cmd}'"

    return msg


def run_container(plan: Plan):
    params = plan.params.copy() if plan.params else []
    if plan.output:
        output_path = join(plan.workdir, plan.output['from']) if 'from' in plan.output else plan.workdir
        params += [{'key': 'OUTPUT', 'value': output_path}]
    update_status(plan, 3, f"Using 1 container for '{plan.identifier}'")
    update_status(plan, 3, __run_container(Plan(
        identifier=plan.identifier,
        plantit_token=plan.plantit_token,
        api_url=plan.api_url,
        workdir=plan.workdir,
        image=plan.image,
        command=plan.command,
        params=params,
        input=plan.input,
        output=plan.output,
        mount=plan.mount
    )))


def run_container_for_directory(plan: Plan, input_directory: str):
    params = (plan.params.copy() if plan.params else []) + [{'key': 'INPUT', 'value': input_directory}]
    if plan.output:
        output_path = join(plan.workdir, plan.output['from']) if plan.output['from'] != '' else plan.workdir
        params += [{'key': 'OUTPUT', 'value': output_path}]
    update_status(plan, 3,
                  f"Using 1 container for '{plan.identifier}' on input directory '{input_directory}'")
    update_status(plan, 3, __run_container(Plan(
        identifier=plan.identifier,
        plantit_token=plan.plantit_token,
        api_url=plan.api_url,
        workdir=plan.workdir,
        image=plan.image,
        command=plan.command,
        params=params,
        input=plan.input,
        output=plan.output,
        mount = plan.mount)))


def run_containers_for_files(plan: Plan, input_directory: str):
    files = os.listdir(input_directory)
    update_status(plan, 3,
                  f"Using {len(files)} container(s) for '{plan.identifier}' on {len(files)} file(s) in input directory '{input_directory}'")
    for file in files:
        params = (copy.deepcopy(plan.params) if plan.params else []) + [
            {'key': 'INPUT', 'value': join(input_directory, file)}]
        output = {}
        if plan.output:
            output = copy.deepcopy(plan.output)
            params += [{'key': 'OUTPUT', 'value': join(plan.workdir, output['from'])}]
        update_status(plan, 3, __run_container(Plan(
            identifier=plan.identifier,
            plantit_token=plan.plantit_token,
            api_url=plan.api_url,
            workdir=plan.workdir,
            image=plan.image,
            command=plan.command,
            params=params,
            input=plan.input,
            output=output,
            mount = plan.mount)))
