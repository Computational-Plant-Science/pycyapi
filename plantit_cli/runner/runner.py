import copy
import os
import subprocess
import traceback
from abc import ABC
from os.path import join, isdir
from pprint import pprint

from dask_jobqueue import SLURMCluster
from distributed import Client, as_completed

from plantit_cli.exceptions import PlantitException
from plantit_cli.config import Config
from plantit_cli.store.store import Store
from plantit_cli.utils import update_status, parse_mount


class Runner(ABC):
    def __init__(self, store: Store):
        self.__store = store

    @staticmethod
    def __clone_repo(config: Config):
        repo_dir = config.clone.rpartition('/')[2]
        if isdir(repo_dir):
            if subprocess.run(f"cd {repo_dir} && git pull",
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE,
                              shell=True).returncode != 0:
                raise PlantitException(f"Repo '{config.clone}' exists on local filesystem, failed to pull updates")
            else:
                update_status(config, 3, f"Updated repo '{config.clone}'")
        elif subprocess.run(f"git clone {config.clone}",
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            shell=True).returncode != 0:
            raise PlantitException(f"Failed to clone repo '{config.clone}'")
        else:
            update_status(config, 3, f"Cloned repo '{config.clone}'")

        if config.branch is not None:
            if subprocess.run(f"cd {repo_dir} && git checkout {config.branch}",
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE,
                              shell=True).returncode != 0:
                raise PlantitException(f"Failed to switch to branch '{config.branch}'")

    def __pull_input(self, config: Config) -> str:
        input_dir = join(config.workdir, 'input')
        input_kind = config.input['kind'].lower()
        os.makedirs(input_dir, exist_ok=True)
        if input_kind == 'directory' or input_kind == 'files':
            self.__store.download_directory(config.input['from'], input_dir,
                                            config.input['filetypes'] if 'filetypes' in config.input else None)
        elif input_kind == 'file':
            self.__store.download_file(config.input['from'], input_dir)
        else:
            raise ValueError(f"'input.kind' must be either 'file' or 'directory'")
        input_files = os.listdir(input_dir)
        if len(input_files) == 0:
            raise PlantitException(f"No inputs found at path '{config.input['from']}'" + (
                f" matching filetypes '{config.input['filetypes']}'" if 'filetypes' in config.input else ''))
        update_status(config, 3, f"Pulled input(s): {', '.join(input_files)}")
        return input_dir

    def __push_output(self, config: Config):
        self.__store.upload_directory(
            join(config.workdir, config.output['from']) if 'from' in config.output else config.workdir,
            config.output['to'],
            (config.output['include']['patterns'] if type(
                config.output['include']['patterns']) is list else None) if 'include' in config.output and 'patterns' in
                                                                            config.output['include'] else None,
            (config.output['include']['names'] if type(
                config.output['include']['names']) is list else None) if 'include' in config.output and 'names' in
                                                                         config.output['include'] else None,
            (config.output['exclude']['patterns'] if type(
                config.output['exclude']['patterns']) is list else None) if 'exclude' in config.output and 'patterns' in
                                                                            config.output['exclude'] else None,
            (config.output['exclude']['names'] if type(
                config.output['exclude']['names']) is list else None) if 'exclude' in config.output and 'names' in
                                                                         config.output['exclude'] else None)

        update_status(config, 3, f"Pushed output(s)")

    @staticmethod
    def __run_command(config: Config):
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
                                    log_file.write(line + '\n')
                                    print(line)
                    else:
                        print(f"Logging output to console")
                        for line in proc.stdout:
                            print(line)

                if proc.returncode:
                    msg = f"Non-zero exit code from command: {cmd}"
                    raise PlantitException(msg)
                else:
                    update_status(config, 3, f"Successfully ran command: {cmd}")
                    return
            except:
                failures += 1
                msg = f"Failed to run command '{cmd}', retrying {max - failures} more time(s): {traceback.format_exc()}"
                update_status(config, 3, msg)
                pass

        update_status(config, 2,
                      f"Failed to run command '{cmd}' after {failures} retries: {traceback.format_exc()}")

    @staticmethod
    def __run_container(config: Config):
        params = config.params.copy() if config.params else []

        if config.output:
            output_path = join(config.workdir, config.output['from']) if 'from' in config.output else config.workdir
            params += [{'key': 'OUTPUT', 'value': output_path}]

        new_config = Config(
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

        update_status(config, 3, f"Running '{config.image}' instance with config: {pprint(new_config)}")
        Runner.__run_command(new_config)

    @staticmethod
    def __run_container_for_directory(config: Config, input_directory: str):
        params = (config.params.copy() if config.params else []) + [{'key': 'INPUT', 'value': input_directory}]

        if config.output:
            output_path = join(config.workdir, config.output['from']) if config.output['from'] != '' else config.workdir
            params += [{'key': 'OUTPUT', 'value': output_path}]

        update_status(config, 3, f"Using 1 container for '{config.identifier}' on input directory '{input_directory}'")
        update_status(config, 3, Runner.__run_command(Config(
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
    def __run_containers_for_files(config: Config, input_directory: str):
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

            update_status(config, 3, Runner.__run_command(Config(
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
    def __run_containers_for_files_slurm(config: Config, input_directory: str):
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

    def run(self, config: Config):
        update_status(config, 3, f"Starting '{config.identifier}'")

        try:
            if config.clone is not None and config.clone != '':
                Runner.__clone_repo(config)

            if config.input:
                print(f"Pulling input(s)")
                input_dir = self.__pull_input(config)
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
                print(f"Pushing output(s)")
                self.__push_output(config)
        except Exception as e:
            update_status(config, 2, f"'{config.identifier}' failed: {traceback.format_exc()}")
            raise e

        update_status(config, 1, f"Completed '{config.identifier}'")
