import copy
import os
import subprocess
import traceback
from abc import ABC
from os.path import join, isdir

from dask_jobqueue import SLURMCluster
from distributed import Client, as_completed
from plantit_cli import commands
from plantit_cli.commands import clone

from plantit_cli.exceptions import PlantitException
from plantit_cli.options import PlantITCLIOptions
from plantit_cli.store.store import Store
from plantit_cli.utils import update_status, parse_mount


class Runner(ABC):
    def __init__(self, store: Store):
        self.__store = store

    @staticmethod
    def __run_command(config: PlantITCLIOptions):
        cmd = f"singularity exec --home {config.workdir}"
        if config.mount is not None:
            if type(config.mount) is list:
                cmd += (' --bind ' + ','.join([parse_mount(config.workdir, mp) for mp in config.mount if mp != '']))
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

        if config.docker_username is not None and config.docker_password is not None:
            print(f"Authenticating with Docker username: {config.docker_username}")
            cmd_with_docker = f"SINGULARITY_DOCKER_USERNAME={config.docker_username} SINGULARITY_DOCKER_PASSWORD={config.docker_password} " + cmd
        else:
            cmd_with_docker = cmd

        failures = 0
        max = 3
        while failures < max:
            try:
                with subprocess.Popen(cmd_with_docker,
                                      stdout=subprocess.PIPE,
                                      stderr=subprocess.STDOUT,
                                      shell=True,
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
                                    log_file.write(line)
                                    print(line)
                    else:
                        print(f"Logging output to console")
                        for line in proc.stdout:
                            print(line)

                if proc.returncode:
                    msg = f"Non-zero exit code from command: {cmd}"
                    raise PlantitException(msg)
                else:
                    return
            except:
                failures += 1
                msg = f"Failed to run command '{cmd}', retrying {max - failures} more time(s): {traceback.format_exc()}"
                update_status(config, 3, msg)
                pass

        update_status(config, 2,
                      f"Failed to run command '{cmd}' after {failures} retries: {traceback.format_exc()}")

    @staticmethod
    def __run_container(config: PlantITCLIOptions):
        params = config.params.copy() if config.params else []

        if config.output:
            output_path = join(config.workdir, config.output['from']) if 'from' in config.output else config.workdir
            params += [{'key': 'OUTPUT', 'value': output_path}]

        new_config = PlantITCLIOptions(
            identifier=config.identifier,
            plantit_token=config.plantit_token,
            docker_username=config.docker_username,
            docker_password=config.docker_password,
            api_url=config.api_url,
            workdir=config.workdir,
            image=config.image,
            command=config.command,
            params=params,
            input=config.input,
            output=config.output,
            mount=config.mount,
            logging=config.logging)

        Runner.__run_command(new_config)

    @staticmethod
    def __run_container_for_directory(config: PlantITCLIOptions, input_directory: str):
        params = (config.params.copy() if config.params else []) + [{'key': 'INPUT', 'value': input_directory}]

        if config.output:
            output_path = join(config.workdir, config.output['from']) if config.output['from'] != '' else config.workdir
            params += [{'key': 'OUTPUT', 'value': output_path}]

        update_status(config, 3, f"Using 1 container for '{config.identifier}' on input directory '{input_directory}'")
        update_status(config, 3, Runner.__run_command(PlantITCLIOptions(
            identifier=config.identifier,
            plantit_token=config.plantit_token,
            docker_username=config.docker_username,
            docker_password=config.docker_password,
            api_url=config.api_url,
            workdir=config.workdir,
            image=config.image,
            command=config.command,
            params=params,
            input=config.input,
            output=config.output,
            mount=config.mount,
            logging=config.logging)))

    @staticmethod
    def __run_containers_for_files(config: PlantITCLIOptions, input_directory: str):
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

            update_status(config, 3, Runner.__run_command(PlantITCLIOptions(
                identifier=config.identifier,
                plantit_token=config.plantit_token,
                docker_username=config.docker_username,
                docker_password=config.docker_password,
                api_url=config.api_url,
                workdir=config.workdir,
                image=config.image,
                command=config.command,
                params=params,
                input=config.input,
                output=output,
                mount=config.mount,
                logging=config.logging)))

    @staticmethod
    def __run_containers_for_files_slurm(config: PlantITCLIOptions, input_directory: str):
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

                new_config = PlantITCLIOptions(
                    identifier=config.identifier,
                    plantit_token=config.plantit_token,
                    docker_username=config.docker_username,
                    docker_password=config.docker_password,
                    api_url=config.api_url,
                    workdir=config.workdir,
                    image=config.image,
                    command=config.command,
                    params=params,
                    input=config.input,
                    output=output,
                    mount=config.mount,
                    logging=config.logging)

                failures = 0
                max = 3
                while failures < max:
                    try:
                        future = client.submit(Runner.__run_container, new_config)
                        update_status(config, 3, f"Submitted file '{file}'")
                        futures.append(future)
                        break
                    except:
                        update_status(config, 3,
                                      f"Failed to submit file '{file}', retrying {max - failures} more time(s)")
                        failures += 1
                        pass

                if failures >= max:
                    update_status(config, 2,
                                  f"Failed to submit file '{file}' after {max} retries")

            finished = 0
            for _ in as_completed(futures):
                finished += 1
                update_status(config, 3, f"Finished {finished} of {len(futures)}")

    def run(self, config: PlantITCLIOptions):
        update_status(config, 3, f"Starting '{config.identifier}'")

        try:
            if config.clone is not None and config.clone != '':
                clone(config)

            if config.input:
                print(f"Pulling input(s)")
                input_dir = commands.pull(
                    self.__store,
                    config.input['from'],
                    join(config.workdir, 'input'),
                    config.input['patterns'] if 'patterns' in config.input else None)
                input_kind = config.input['kind'].lower()
                if input_kind == 'directory':
                    Runner.__run_container_for_directory(config, input_dir)
                elif input_kind == 'files' or input_kind == 'file':
                    if config.slurm is not None:
                        Runner.__run_containers_for_files_slurm(config, input_dir)
                    else:
                        Runner.__run_containers_for_files(config, input_dir)
                else:
                    raise ValueError(f"'input.kind' must be 'file', 'files', or 'directory'")
            else:
                Runner.__run_container(config)

            if config.output:
                from_path = join(config.workdir, config.output['from']) if 'from' in config.output else config.workdir
                commands.zip(
                    path=from_path,
                    name=config.identifier,
                    include_patterns=config.output['include']['patterns'] if 'include' in config.output and 'patterns' in config.output[
                        'include'] else None,
                    include_names=config.output['include']['names'] if 'include' in config.output and 'names' in config.output['include'] else None,
                    exclude_patterns=config.output['exclude']['patterns'] if 'exclude' in config.output and 'patterns' in config.output[
                        'exclude'] else None,
                    exclude_names=config.output['exclude']['names'] if 'exclude' in config.output and 'names' in config.output['exclude'] else None)
                if 'to' in config.output:
                    commands.push(
                        store=self.__store,
                        from_path=from_path,
                        to_path=config.output['to'],
                        include_patterns=config.output['include']['patterns'] if 'include' in config.output and 'patterns' in config.output['include'] else None,
                        include_names=config.output['include']['names'] if 'include' in config.output and 'names' in config.output['include'] else None,
                        exclude_patterns=config.output['exclude']['patterns'] if 'exclude' in config.output and 'patterns' in config.output['exclude'] else None,
                        exclude_names=config.output['exclude']['names'] if 'exclude' in config.output and 'names' in config.output['exclude'] else None)
        except Exception as e:
            update_status(config, 2, f"'{config.identifier}' failed: {traceback.format_exc()}")
            raise e

        update_status(config, 1, f"Completed '{config.identifier}'")
