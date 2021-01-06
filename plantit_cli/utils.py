import os
import copy
import subprocess
from os import listdir
from os.path import join, isfile, isdir

import requests
from dask_jobqueue.slurm import SLURMCluster
from distributed import Client, as_completed

from plantit_cli.exceptions import PlantitException
from plantit_cli.config import Config


def list_files(path,
               include_patterns=None,
               include_names=None,
               exclude_patterns=None,
               exclude_names=None):
    # gather all files
    all_paths = [join(path, file) for file in listdir(path) if isfile(join(path, file))]

    # add files matching included patterns
    included_by_pattern = [pth for pth in all_paths if any(
        pattern.lower() in pth.lower() for pattern in include_patterns)] if include_patterns is not None else all_paths

    # add files included by name
    included_by_name = [pth for pth in all_paths if pth.split('/')[-1] in [name for name in
                                                                           include_names]] if include_names is not None else included_by_pattern

    # gather all included files
    included = set(included_by_pattern + included_by_name)

    # remove files matched excluded patterns
    excluded_by_pattern = [name for name in included if all(pattern.lower() not in name.lower() for pattern in
                                                            exclude_patterns)] if exclude_patterns is not None else included

    # remove files excluded by name
    excluded_by_name = [pattern for pattern in excluded_by_pattern if pattern.split('/')[
        -1] not in exclude_names] if exclude_names is not None else excluded_by_pattern

    return excluded_by_name


def update_status(config: Config, state: int, description: str):
    print(description)
    if config.api_url:
        requests.post(
            config.api_url,
            data={
                'run_id': config.identifier,
                'state': state,
                'description': description
            },
            headers={"Authorization": f"Token {config.plantit_token}"})


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


def validate_config(config: Config):
    errors = []

    # identifier
    if config.identifier == '':
        errors.append('Attribute \'identifier\' must not be empty')

    # image
    if config.image == '':
        errors.append('Attribute \'image\' must not be empty')
    else:
        container_split = config.image.split('/')
        container_name = container_split[-1]
        container_owner = None if container_split[-2] == '' else container_split[-2]
        if 'docker' in config.image:
            if not docker_container_exists(container_name, container_owner):
                errors.append(f"Image '{config.image}' not found on Docker Hub")

    # working directory
    if config.workdir == '':
        errors.append('Attribute \'workdir\' must not be empty')
    elif not isdir(config.workdir):
        errors.append(f"Working directory '{config.workdir}' does not exist")

    # command
    if config.command == '':
        errors.append('Attribute \'command\' must not be empty')

    # params
    if config.params is not None and not all(['key' in param and
                                              param['key'] is not None and
                                              param['key'] != '' and
                                              'value' in param and
                                              param['value'] is not None and
                                              param['value'] != ''
                                              for param in config.params]):
        errors.append('Every parameter must have a non-empty \'key\' and \'value\'')

    # input
    if config.input is not None:
        # token
        if config.cyverse_token is None or config.cyverse_token == '':
            errors.append(f"CyVerse token must be provided")

        # kind
        if 'kind' not in config.input:
            errors.append('Attribute \'input\' must include attribute \'kind\'')
        elif type(config.input['kind']) is not str or not (
                config.input['kind'] == 'file' or config.input['kind'] == 'files' or config.input[
            'kind'] == 'directory'):
            errors.append('Attribute \'input.kind\' must be either \'file\', \'files\', or \'directory\'')

        # from
        if 'from' not in config.input:
            errors.append('Attribute \'input\' must include attribute \'from\'')
        elif type(config.input['from']) is not str or not cyverse_path_exists(config.input['from'],
                                                                              config.cyverse_token):
            errors.append(f"Attribute 'input.from' must be a valid path in the CyVerse Data Store")

        # overwrite
        if 'overwrite' in config.input and type(config.input['overwrite']) is not bool:
            errors.append('Attribute \'input.overwrite\' must be a bool')

        # patterns
        if 'patterns' in config.input and not all(
                type(pattern) is str and pattern != '' for pattern in config.input['patterns']):
            errors.append('Attribute \'input.patterns\' must be a non-empty str')

    # output
    if config.output is not None:
        # token
        if config.cyverse_token is None or config.cyverse_token == '':
            errors.append(f"CyVerse token must be provided")

        # from
        if 'from' not in config.output:
            errors.append('Attribute \'output\' must include attribute \'from\'')
        elif type(config.output['from']) is not str:
            errors.append(f"Attribute 'output.from' must be a str")

        # to
        if 'to' not in config.output:
            errors.append('Attribute \'output\' must include attribute \'to\'')
        elif type(config.output['to']) is not str:
            errors.append(f"Attribute 'output.from' must be a valid path in the CyVerse Data Store")

        # include
        if 'include' in config.output:
            if 'patterns' in config.output['include']:
                if type(config.output['include']['patterns']) is not list or not all(
                        type(pattern) is str for pattern in config.output['include']['patterns']):
                    errors.append('Attribute \'output.include.patterns\' must be a list of str')
            if 'names' in config.output['include']:
                if type(config.output['include']['names']) is not list or not all(
                        type(name) is str for name in config.output['include']['names']):
                    errors.append('Attribute \'output.include.names\' must be a list of str')

        # exclude
        if 'exclude' in config.output:
            if 'patterns' in config.output['exclude']:
                if type(config.output['exclude']['patterns']) is not list or not all(
                        type(pattern) is str for pattern in config.output['exclude']['patterns']):
                    errors.append('Attribute \'output.exclude.patterns\' must be a list of str')
            if 'names' in config.output['exclude']:
                if type(config.output['exclude']['names']) is not list or not all(
                        type(name) is str for name in config.output['exclude']['names']):
                    errors.append('Attribute \'output.exclude.names\' must be a list of str')

    # logging
    if config.logging is not None:
        if 'file' in config.logging and type(config.logging['file']) is not str:
            errors.append('Attribute \'output.logging.file\' must be a str')
        elif config.logging['file'].rpartition('/')[0] != '' and not isdir(config.logging['file'].rpartition('/')[0]):
            errors.append('Attribute \'output.logging.file\' must be a valid file path')

    return True if len(errors) == 0 else (False, errors)


def __parse_mount(workdir, mp):
    return mp.rpartition(':')[0] + ':' + mp.rpartition(':')[2] if ':' in mp else workdir + ':' + mp


def __run_container(config: Config):
    cmd = f"singularity exec --home {config.workdir}"
    if config.mount is not None:
        if type(config.mount) is list:
            cmd += (' --bind ' + ','.join([__parse_mount(config.workdir, mp) for mp in config.mount if mp != '']))
        else:
            update_status(config, 3, f"List expected for `mount`")

    cmd += f" {config.image} {config.command}"
    for param in sorted(config.params, key=lambda p: len(p['key']), reverse=True):
        cmd = cmd.replace(f"${param['key'].upper()}", param['value'])
    if config.input is not None and 'filetypes' in config.input and config.input['filetypes'] is not None:
        if len(config.input['filetypes']) > 0:
            cmd = cmd.replace("$FILETYPES", ','.join(config.input['filetypes']))
        else:
            cmd = cmd.replace("$FILETYPES", '')

    msg = f"Running container with command: '{cmd}'"
    update_status(config, 3, msg)

    with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True,
                          universal_newlines=True) as proc:
        if config.logging is not None and 'file' in config.logging:
            log_file_path = config.logging['file']
            log_file_dir = os.path.split(log_file_path)[0]
            if log_file_dir is not None and log_file_dir != '' and not isdir(log_file_dir):
                raise FileNotFoundError(f"Directory does not exist: {log_file_dir}")
            else:
                print(f"Logging output to file '{log_file_path}'")
                with open(log_file_path, 'a') as log_file:
                    for line in proc.stdout:
                        log_file.write(line + '\n')
                        print(line)
        else:
            print(f"Logging output to console")
            for line in proc.stdout:
                print(line)

    if proc.returncode:
        msg = f"Non-zero exit code from command: '{cmd}'"
        update_status(config, 2, msg)
        raise PlantitException(msg)
    else:
        msg = f"Successfully ran command: '{cmd}'"

    return msg


def run_container(config: Config):
    params = config.params.copy() if config.params else []

    if config.output:
        output_path = join(config.workdir, config.output['from']) if 'from' in config.output else config.workdir
        params += [{'key': 'OUTPUT', 'value': output_path}]

    update_status(config, 3, f"Using 1 container for '{config.identifier}'")
    update_status(config, 3, __run_container(Config(
        identifier=config.identifier,
        plantit_token=config.plantit_token,
        api_url=config.api_url,
        workdir=config.workdir,
        image=config.image,
        command=config.command,
        params=params,
        input=config.input,
        output=config.output,
        mount=config.mount,
        logging=config.logging
    )))


def run_container_for_directory(config: Config, input_directory: str):
    params = (config.params.copy() if config.params else []) + [{'key': 'INPUT', 'value': input_directory}]

    if config.output:
        output_path = join(config.workdir, config.output['from']) if config.output['from'] != '' else config.workdir
        params += [{'key': 'OUTPUT', 'value': output_path}]

    update_status(config, 3, f"Using 1 container for '{config.identifier}' on input directory '{input_directory}'")
    update_status(config, 3, __run_container(Config(
        identifier=config.identifier,
        plantit_token=config.plantit_token,
        api_url=config.api_url,
        workdir=config.workdir,
        image=config.image,
        command=config.command,
        params=params,
        input=config.input,
        output=config.output,
        mount=config.mount,
        logging=config.logging)))


def run_containers_for_files(config: Config, input_directory: str):
    files = os.listdir(input_directory)
    update_status(config, 3,
                  f"Using {len(files)} container(s) for '{config.identifier}' on {len(files)} file(s) in input directory '{input_directory}'")

    for file in files:
        params = (copy.deepcopy(config.params) if config.params else []) + [
            {'key': 'INPUT', 'value': join(input_directory, file)}]

        output = {}
        if config.output:
            output = copy.deepcopy(config.output)
            params += [{'key': 'OUTPUT', 'value': join(config.workdir, output['from'])}]

        update_status(config, 3, __run_container(Config(
            identifier=config.identifier,
            plantit_token=config.plantit_token,
            api_url=config.api_url,
            workdir=config.workdir,
            image=config.image,
            command=config.command,
            params=params,
            input=config.input,
            output=output,
            mount=config.mount,
            logging=config.logging)))


def run_containers_for_files_slurm(config: Config, input_directory: str):
    files = os.listdir(input_directory)
    cluster = SLURMCluster(**config.slurm)
    nodes = len(files)
    cluster.scale(nodes)
    print(f"Using job script: {cluster.job_script()}")
    update_status(config, 3,
                  f"Requesting {nodes}-node cluster to process {nodes} files in '{input_directory}'")

    with Client(cluster) as client:
        futures = []
        for file in files:
            params = (copy.deepcopy(config.params) if config.params else []) + [
                {'key': 'INPUT', 'value': join(input_directory, file)}]

            output = {}
            if config.output:
                output = copy.deepcopy(config.output)
                params += [{'key': 'OUTPUT', 'value': join(config.workdir, output['from'])}]

            new_config = Config(
                identifier=config.identifier,
                plantit_token=config.plantit_token,
                api_url=config.api_url,
                workdir=config.workdir,
                image=config.image,
                command=config.command,
                params=params,
                input=config.input,
                output=output,
                mount=config.mount,
                logging=config.logging)

            future = client.submit(__run_container, new_config)
            update_status(config, 3, f"Submitted file '{file}' for processing")
            futures.append(future)

        for future in as_completed(futures):
            update_status(config, 3, future.result())
