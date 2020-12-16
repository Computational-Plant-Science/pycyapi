import os
import copy
import subprocess
from os import listdir
from os.path import join, isfile, isdir

import requests

from plantit_cli.exceptions import PlantitException
from plantit_cli.plan import Plan


def list_files(path, include_pattern=None, include=None, exclude_pattern=None, exclude=None):
    all_paths = [join(path, file) for file in listdir(path) if isfile(join(path, file))]
    included_by_pattern = [p for p in all_paths if include_pattern.lower() in p.lower()] if include_pattern is not None else all_paths
    included_by_name = [p for p in all_paths if
                     p.split('/')[-1] in [pi for pi in include]] if include is not None else included_by_pattern
    included = set(included_by_pattern + included_by_name)
    excluded_by_pattern = [p for p in included if exclude_pattern.lower() not in p.lower()] if exclude_pattern is not None else included
    excluded = [p for p in excluded_by_pattern if p.split('/')[-1] not in exclude] if exclude is not None else excluded_by_pattern

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


def __parse_mount(workdir, mp):
    return mp.rpartition(':')[0] + ':' + mp.rpartition(':')[2] if ':' in mp else workdir + ':' + mp


def __run_container(plan: Plan):
    cmd = f"singularity exec --home {plan.workdir}"
    if plan.mount is not None:
        if type(plan.mount) is list:
            cmd += (' --bind ' + ','.join([__parse_mount(plan.workdir, mp) for mp in plan.mount if mp != '']))
        else:
            update_status(plan, 3, f"List expected for `mount`")
    cmd += f" {plan.image} {plan.command}"
    for param in sorted(plan.params, key=lambda p: len(p['key']), reverse=True):
        cmd = cmd.replace(f"${param['key'].upper()}", param['value'])
    msg = f"Running '{cmd}'"
    update_status(plan, 3, msg)

    with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True, universal_newlines=True) as proc:
        if plan.logging is not None and 'file' in plan.logging:
            log_file_path = plan.logging['file']
            log_file_dir = os.path.split(log_file_path)[0]
            if log_file_dir is not None and log_file_dir != '' and not isdir(log_file_dir):
                raise FileNotFoundError(f"Directory does not exist: {log_file_dir}")
            else:
                print(f"Logging container output to file '{log_file_path}'")
                with open(log_file_path, 'a') as log_file:
                    for line in proc.stdout:
                        log_file.write(line + '\n')
                        print(line)
        else:
            print(f"Logging container output to console")
            for line in proc.stdout:
                print(line)

    if proc.returncode:
        msg = f"Non-zero exit code from container"
        update_status(plan, 2, msg)
        raise PlantitException(msg)
    else:
        msg = f"Successfully ran container"

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
        mount=plan.mount,
        logging=plan.logging
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
        mount=plan.mount,
        logging=plan.logging)))


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
            mount=plan.mount,
            logging=plan.logging)))
