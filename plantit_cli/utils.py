import os
import copy
import subprocess
from os import listdir
from os.path import join, isfile, isdir

import requests

from plantit_cli.exceptions import PlantitException
from plantit_cli.plan import Plan


def list_files(path,
               include_patterns=None,
               include_names=None,
               exclude_patterns=None,
               exclude_names=None):
    all_paths = [join(path, file) for file in listdir(path) if isfile(join(path, file))]
    included_by_pattern = [pth for pth in all_paths if any(
        pattern.lower() in pth.lower() for pattern in include_patterns)] if include_patterns is not None else all_paths
    included_by_name = [pth for pth in all_paths if pth.split('/')[-1] in [name for name in
                                                                           include_names]] if include_names is not None else included_by_pattern
    included = set(included_by_pattern + included_by_name)
    excluded_by_pattern = [name for name in included if all(pattern.lower() not in name.lower() for pattern in
                                                            exclude_patterns)] if exclude_patterns is not None else included
    excluded_by_name = [pattern for pattern in excluded_by_pattern if pattern.split('/')[
        -1] not in exclude_names] if exclude_names is not None else excluded_by_pattern

    return excluded_by_name


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


def docker_container_exists(name, owner=None):
    content = requests.get(
        f"https://hub.docker.com/v2/repositories/{owner if owner is not None else 'library'}/{name}/").json()
    if 'user' not in content or 'name' not in content:
        return False
    if content['user'] != (owner if owner is not None else 'library') or content['name'] != name:
        return False
    return True


def cyverse_path_exists(path, token):
    response = requests.get(f"https://de.cyverse.org/terrain/secured/filesystem/paged-directory?limit=1000&path={path}",
                            headers={"Authorization": f"Bearer {token}"})
    content = response.json()
    input_type = 'directory'
    if response.status_code != 200:
        if 'error_code' not in content or ('error_code' in content and content['error_code'] == 'ERR_DOES_NOT_EXIST'):
            path_split = path.rpartition('/')
            base = path_split[0]
            file = path_split[2]
            up_response = requests.get(
                f"https://de.cyverse.org/terrain/secured/filesystem/paged-directory?limit=1000&path={base}",
                headers={"Authorization": f"Bearer {token}"})
            up_content = up_response.json()
            if up_response.status_code != 200:
                if 'error_code' not in up_content:
                    print(f"Unknown error: {up_content}")
                    return False
                elif 'error_code' in up_content:
                    print(f"Error: {up_content['error_code']}")
                    return False
            elif 'files' not in up_content:
                print(f"Directory '{base}' does not exist")
                return False
            elif len(up_content['files']) > 1:
                print(f"Multiple files found in directory '{base}' matching name '{file}'")
                return False
            elif len(up_content['files']) == 0 or up_content['files'][0]['label'] != file:
                print(f"File '{file}' does not exist in directory '{base}'")
                return False
            else:
                input_type = 'file'
        else:
            return False
    return True, input_type


def validate_plan(plan: Plan):
    errors = []

    # identifier
    if plan.identifier == '':
        errors.append('Attribute \'identifier\' must not be empty')

    # image
    if plan.image == '':
        errors.append('Attribute \'image\' must not be empty')
    else:
        container_split = plan.image.split('/')
        container_name = container_split[-1]
        container_owner = None if container_split[-2] == '' else container_split[-2]
        if not docker_container_exists(container_name, container_owner):
            errors.append(f"Image '{plan.image}' not found on Docker Hub")

    # working directory
    if plan.workdir == '':
        errors.append('Attribute \'workdir\' must not be empty')
    elif not isdir(plan.workdir):
        errors.append(f"Working directory '{plan.workdir}' does not exist")

    # command
    if plan.command == '':
        errors.append('Attribute \'command\' must not be empty')

    # params
    if plan.params is not None and not all(['key' in param and
                                            param['key'] is not None and
                                            param['key'] != '' and
                                            'value' in param and
                                            param['value'] is not None and
                                            param['value'] != ''
                                            for param in plan.params]):
        errors.append('Every parameter must have a non-empty \'key\' and \'value\'')

    # input
    if plan.input is not None:
        # token
        if plan.cyverse_token is None or plan.cyverse_token == '':
            errors.append(f"CyVerse token must be provided")

        # kind
        if 'kind' not in plan.input:
            errors.append('Attribute \'input\' must include attribute \'kind\'')
        elif type(plan.input['kind']) is not str or not (
                plan.input['kind'] == 'file' or plan.input['kind'] == 'files' or plan.input['kind'] == 'directory'):
            errors.append('Attribute \'input.kind\' must be either \'file\', \'files\', or \'directory\'')

        # from
        if 'from' not in plan.input:
            errors.append('Attribute \'input\' must include attribute \'from\'')
        elif type(plan.input['from']) is not str or not cyverse_path_exists(plan.input['from'], plan.cyverse_token):
            errors.append(f"Attribute 'input.from' must be a valid path in the CyVerse Data Store")

        # overwrite
        if 'overwrite' in plan.input and type(plan.input['overwrite']) is not bool:
            errors.append('Attribute \'input.overwrite\' must be a bool')

        # patterns
        if 'patterns' in plan.input and not all(type(pattern) is str and pattern != '' for pattern in plan.input['patterns']):
            errors.append('Attribute \'input.patterns\' must be a non-empty str')

    # output
    if plan.output is not None:
        # token
        if plan.cyverse_token is None or plan.cyverse_token == '':
            errors.append(f"CyVerse token must be provided")

        # from
        if 'from' not in plan.output:
            errors.append('Attribute \'output\' must include attribute \'from\'')
        elif type(plan.output['from']) is not str:
            errors.append(f"Attribute 'output.from' must be a str")

        # to
        if 'to' not in plan.output:
            errors.append('Attribute \'output\' must include attribute \'to\'')
        elif type(plan.output['to']) is not str:
            errors.append(f"Attribute 'output.from' must be a valid path in the CyVerse Data Store")

        # include
        if 'include' in plan.output:
            if 'patterns' in plan.output['include']:
                if type(plan.output['include']['patterns']) is not list or not all(type(pattern) is str for pattern in plan.output['include']['patterns']):
                    errors.append('Attribute \'output.include.patterns\' must be a list of str')
            if 'names' in plan.output['include']:
                if type(plan.output['include']['names']) is not list or not all(type(name) is str for name in plan.output['include']['names']):
                    errors.append('Attribute \'output.include.names\' must be a list of str')

        # exclude
        if 'exclude' in plan.output:
            if 'patterns' in plan.output['exclude']:
                if type(plan.output['exclude']['patterns']) is not list or not all(type(pattern) is str for pattern in plan.output['exclude']['patterns']):
                    errors.append('Attribute \'output.exclude.patterns\' must be a list of str')
            if 'names' in plan.output['exclude']:
                if type(plan.output['exclude']['names']) is not list or not all(type(name) is str for name in plan.output['exclude']['names']):
                    errors.append('Attribute \'output.exclude.names\' must be a list of str')

    # logging
    if plan.logging is not None:
        if 'file' in plan.logging and type(plan.logging['file']) is not str:
            errors.append('Attribute \'output.logging.file\' must be a str')
        elif plan.logging['file'].rpartition('/')[0] != '' and not isdir(plan.logging['file'].rpartition('/')[0]):
            errors.append('Attribute \'output.logging.file\' must be a valid file path')

    return True if len(errors) == 0 else (False, errors)


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

    with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True,
                          universal_newlines=True) as proc:
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
