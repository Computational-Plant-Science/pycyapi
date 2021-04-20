import subprocess
import traceback
from os import listdir
from os.path import join, isfile, isdir
from time import sleep
from typing import List

import requests
from distributed import Client

from plantit_cli.options import BindMount, Parameter, FileInput, FilesInput, DirectoryInput, RunOptions


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
    included_by_name = ([pth for pth in all_paths if pth.rpartition('/')[2] in [name for name in include_names]] \
                            if include_names is not None else included_by_pattern) + \
                       [pth for pth in all_paths if pth in [name for name in include_names]] \
        if include_names is not None else included_by_pattern

    # gather only included files
    included = set(included_by_pattern + included_by_name)

    # remove files matched excluded patterns
    excluded_by_pattern = [name for name in included if all(pattern.lower() not in name.lower() for pattern in
                                                            exclude_patterns)] if exclude_patterns is not None else included

    # remove files excluded by name
    excluded_by_name = [pattern for pattern in excluded_by_pattern if pattern.split('/')[
        -1] not in exclude_names] if exclude_names is not None else excluded_by_pattern

    return excluded_by_name


def parse_docker_image_components(value):
    container_split = value.split('/')
    container_name = container_split[-1]
    container_owner = None if container_split[-2] == '' else container_split[-2]
    if ':' in container_name:
        container_name_split = container_name.split(":")
        container_name = container_name_split[0]
        container_tag = container_name_split[1]
    else:
        container_tag = None

    return container_owner, container_name, container_tag


def parse_options(raw: dict):
    errors = []

    image = None
    if not isinstance(raw['image'], str):
        errors.append('Attribute \'image\' must not be a str')
    elif raw['image'] == '':
        errors.append('Attribute \'image\' must not be empty')
    else:
        image = raw['image']
        if 'docker' in image:
            image_owner, image_name, image_tag = parse_docker_image_components(image)
            if not docker_image_exists(image_name, image_owner, image_tag):
                errors.append(f"Image '{image}' not found on Docker Hub")

    work_dir = None
    if not isinstance(raw['workdir'], str):
        errors.append('Attribute \'workdir\' must not be a str')
    elif raw['workdir'] == '':
        errors.append('Attribute \'workdir\' must not be empty')
    elif not isdir(raw['workdir']):
        errors.append(f"Working directory '{raw['workdir']}' does not exist")
    else:
        work_dir = raw['workdir']

    command = None
    if not isinstance(raw['command'], str):
        errors.append('Attribute \'command\' must not be a str')
    elif raw['command'] == '':
        errors.append('Attribute \'command\' must not be empty')
    else:
        command = raw['command']

    parameters = None
    if 'parameters' in raw:
        if not all(['key' in param and
                    param['key'] is not None and
                    param['key'] != '' and
                    'value' in param and
                    param['value'] is not None and
                    param['value'] != ''
                    for param in raw['parameters']]):
            errors.append('Every parameter must have a non-empty \'key\' and \'value\'')
        else:
            parameters = [Parameter(param['key'], param['value']) for param in raw['parameters']]

    bind_mounts = None
    if 'bind_mounts' in raw:
        if not all (mount_point != '' for mount_point in raw['bind_mounts']):
            errors.append('Every mount point must be non-empty')
        else:
            bind_mounts = [parse_bind_mount(work_dir, mount_point) for mount_point in raw['bind_mounts']]

    input = None
    if 'input' in raw:
        if 'file' in raw['input']:
            if 'path' not in raw['input']['file']:
                errors.append('Section \'file\' must include attribute \'path\'')
            input = FileInput(path=raw['input']['file']['path'])
        elif 'files' in raw['input']:
            if 'path' not in raw['input']['files']:
                errors.append('Section \'files\' must include attribute \'path\'')
            input = FilesInput(
                path=raw['input']['files']['path'],
                patterns=raw['input']['files']['patterns'] if 'patterns' in raw['input']['files'] else None)
        elif 'directory' in raw['input']:
            if 'path' not in raw['input']['directory']:
                errors.append('Section \'directory\' must include attribute \'path\'')
            input = DirectoryInput(path=raw['input']['directory']['path'])
        else:
            errors.append('Section \'input\' must include a \'file\', \'files\', or \'directory\' section')

    log_file = None
    if 'log_file' in raw:
        log_file = raw['log_file']
        if not isinstance(log_file, str):
            errors.append('Attribute \'log_file\' must be a str')
        elif log_file.rpartition('/')[0] != '' and not isdir(log_file.rpartition('/')[0]):
            errors.append('Attribute \'log_file\' must be a valid file path')

    no_cache = None
    if 'no_cache' in raw:
        no_cache = raw['no_cache']
        if not isinstance(no_cache, bool):
            errors.append('Attribute \'no_cache\' must be a bool')

    gpu = None
    if 'gpu' in raw:
        gpu = raw['gpu']
        if not isinstance(gpu, bool):
            errors.append('Attribute \'gpu\' must be a bool')

    jobqueue = None
    if 'jobqueue' in raw:
        jobqueue = raw['jobqueue']
        if not ('slurm' in jobqueue or 'yarn' in jobqueue or 'pbs' in jobqueue or 'moab' in jobqueue or 'sge' in jobqueue or 'lsf' in jobqueue or 'oar' in jobqueue or 'kube' in jobqueue):
            raise ValueError(f"Unsupported jobqueue configuration: {jobqueue}")

        if 'queue' in jobqueue:
            if not isinstance(jobqueue['queue'], str):
                errors.append('Section \'jobqueue\'.\'queue\' must be a str')
        if 'project' in jobqueue:
            if not isinstance(jobqueue['project'], str):
                errors.append('Section \'jobqueue\'.\'project\' must be a str')
        if 'walltime' in jobqueue:
            if not isinstance(jobqueue['walltime'], str):
                errors.append('Section \'jobqueue\'.\'walltime\' must be a str')
        if 'cores' in jobqueue:
            if not isinstance(jobqueue['cores'], int):
                errors.append('Section \'jobqueue\'.\'cores\' must be a int')
        if 'processes' in jobqueue:
            if not isinstance(jobqueue['processes'], int):
                errors.append('Section \'jobqueue\'.\'processes\' must be a int')
        if 'extra' in jobqueue and not all(extra is str for extra in jobqueue['extra']):
            errors.append('Section \'jobqueue\'.\'extra\' must be a list of str')
        if 'header_skip' in jobqueue and not all(extra is str for extra in jobqueue['header_skip']):
            errors.append('Section \'jobqueue\'.\'header_skip\' must be a list of str')

    return errors, RunOptions(
        workdir=work_dir,
        image=image,
        command=command,
        input=input,
        parameters=parameters,
        bind_mounts=bind_mounts,
        # checksums=checksums,
        log_file=log_file,
        jobqueue=jobqueue,
        no_cache=no_cache,
        gpu=gpu)


def update_status(state: int, description: str, api_url: str = None, api_token: str = None, retries: int = 3):
    print(description)
    if api_url is not None and api_url != '':
        if api_token is None or api_token == '':
            raise ValueError(f"You must provide a PlantIT API access token if you provide an API URL")

        failures = 1
        while failures <= retries:
            try:
                requests.post(
                    api_url,
                    data={'state': int(state), 'description': description},
                    headers={"Authorization": f"Token {api_token}"})
                break
            except Exception:
                print(traceback.format_exc())
                if failures > retries:
                    print(f"Failed to report status to PlantIT after {failures} failures")
                    break
                sleep(failures * 0.5)
                failures += 1


def docker_image_exists(name, owner=None, tag=None):
    url = f"https://hub.docker.com/v2/repositories/{owner if owner is not None else 'library'}/{name}/"
    if tag is not None:
        url += f"tags/{tag}/"
    response = requests.get(url)
    try:
        content = response.json()
        if 'user' not in content and 'name' not in content:
            return False
        if content['name'] != tag and content['name'] != name and content['user'] != (owner if owner is not None else 'library'):
            return False
        return True
    except:
        return False


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


def prep_command(
        work_dir: str,
        image: str,
        command: str,
        bind_mounts: List[BindMount] = None,
        parameters: List[Parameter] = None,
        docker_username: str = None,
        docker_password: str = None,
        no_cache: bool = False,
        gpu: bool = False):
    cmd = f"singularity exec --home {work_dir}"

    if bind_mounts is not None:
        if len(bind_mounts) > 0:
            cmd += (' --bind ' + ','.join([format_bind_mount(work_dir, mount_point) for mount_point in bind_mounts]))
        else:
            raise ValueError(f"List expected for `bind_mounts`")

    if parameters is None:
        parameters = []
    parameters.append(Parameter(key='WORKDIR', value=work_dir))
    for parameter in parameters:
        print(f"Replacing '{parameter.key.upper()}' with '{parameter.value}'")
        command = command.replace(f"${parameter.key.upper()}", parameter.value)

    command = command.replace("$GPU_MODE", 'true' if gpu else 'false')

    if no_cache:
        cmd += ' --disable-cache'

    if gpu:
        cmd += ' --nv'

    cmd += f" {image} {command}"
    print(f"Using command: '{cmd}'")

    # wait until now to add env variables so we don't reveal auth info to the end user
    if docker_username is not None and docker_password is not None:
        cmd = f"SINGULARITY_DOCKER_USERNAME={docker_username} SINGULARITY_DOCKER_PASSWORD={docker_password} " + cmd

    return cmd


def run_command(command: str, log_file: str = None, retries: int = 3):
    failures = 0
    while failures < retries:
        try:
            with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True, universal_newlines=True) as proc:
                if log_file is not None and log_file != '':
                    print(f"Logging output to file: {log_file}")

                    with open(log_file, 'a') as log:
                        for line in proc.stdout:
                            print(line.strip())
                            log.write(line)
                else:
                    print(f"Logging output to console")
                    for line in proc.stdout:
                        print(line.strip())

            if proc.returncode:
                raise RuntimeError(f"Non-zero exit code")
            else:
                msg = 'Command succeeded'
                print(msg)
                return msg
        except:
            failures += 1
            print(f"Command failed, retrying {retries - failures} more time(s): {traceback.format_exc()}")
            pass

    msg = f"Aborting command after {failures} failures: {traceback.format_exc()}"
    print(msg)
    return msg


# noinspection PyBroadException
def submit_command(client: Client, command: str, log_file: str, retries: int = 3):
    failures = 0
    while failures < retries:
        try:
            return client.submit(run_command, command, log_file, retries)
        except:
            print(f"Failed to submit container to Dask, retrying {retries - failures} more time(s): {traceback.format_exc()}")
            failures += 1
            pass

    if failures >= retries:
        raise RuntimeError(f"Failed to submit container to Dask after {retries} retries")


def parse_flow_repo(repo: str):
    split = repo.rpartition('/')
    if len(split) != 3:
        raise ValueError(f"Malformed repository identifier: {repo}")

    owner = split[0]
    name = split[2]
    return owner, name


def parse_bind_mount(workdir: str, bind_mount: str):
    split = bind_mount.rpartition(':')
    return BindMount(host_path=split[0], container_path=split[2]) if len(split) > 0 else BindMount(host_path=workdir, container_path=bind_mount)


def format_bind_mount(workdir: str, bind_mount: BindMount):
    return bind_mount.host_path + ':' + bind_mount.container_path if bind_mount.host_path != '' else workdir + ':' + bind_mount.container_path


BYTE_SYMBOLS = {
    'customary': ('B', 'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'),
    'customary_ext': ('byte', 'kilo', 'mega', 'giga', 'tera', 'peta', 'exa',
                      'zetta', 'iotta'),
    'iec': ('Bi', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi', 'Yi'),
    'iec_ext': ('byte', 'kibi', 'mebi', 'gibi', 'tebi', 'pebi', 'exbi',
                'zebi', 'yobi'),
}


def readable_bytes(n, format='%(value).1f %(symbol)s', symbols='customary'):
    """
    Convert n bytes into a human readable string based on format.
    symbols can be either "customary", "customary_ext", "iec", or "iec_ext".

    Referenced from https://stackoverflow.com/a/13449587.

      >>> readable_bytes(0)
      '0.0 B'
      >>> readable_bytes(0.9)
      '0.0 B'
      >>> readable_bytes(1)
      '1.0 B'
      >>> readable_bytes(1.9)
      '1.0 B'
      >>> readable_bytes(1024)
      '1.0 K'
      >>> readable_bytes(1048576)
      '1.0 M'
      >>> readable_bytes(1099511627776127398123789121)
      '909.5 Y'

      >>> readable_bytes(9856, symbols="customary")
      '9.6 K'
      >>> readable_bytes(9856, symbols="customary_ext")
      '9.6 kilo'
      >>> readable_bytes(9856, symbols="iec")
      '9.6 Ki'
      >>> readable_bytes(9856, symbols="iec_ext")
      '9.6 kibi'

      >>> readable_bytes(10000, "%(value).1f %(symbol)s/sec")
      '9.8 K/sec'

      >>> # precision can be adjusted by playing with %f operator
      >>> readable_bytes(10000, format="%(value).5f %(symbol)s")
      '9.76562 K'
    """

    n = int(n)
    if n < 0:
        raise ValueError("n < 0")

    symbols = BYTE_SYMBOLS[symbols]
    prefix = {}
    for i, s in enumerate(symbols[1:]):
        prefix[s] = 1 << (i + 1) * 10

    for symbol in reversed(symbols[1:]):
        if n >= prefix[symbol]:
            value = float(n) / prefix[symbol]
            return format % locals()

    return format % dict(symbol=symbols[0], value=n)
