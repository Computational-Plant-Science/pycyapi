import os
import subprocess
import traceback
from abc import ABC
from os.path import join, isdir

from plantit_cli.exceptions import PlantitException
from plantit_cli.config import Config
from plantit_cli.store.store import Store
from plantit_cli.utils import update_status, run_container_for_directory, run_containers_for_files, \
    run_container, run_containers_for_files_slurm


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
                    run_container_for_directory(config, input_dir)
                elif input_kind == 'files' or input_kind == 'file':
                    if config.slurm is not None:
                        run_containers_for_files_slurm(config, input_dir)
                    else:
                        run_containers_for_files(config, input_dir)
                else:
                    raise ValueError(f"'input.kind' must be 'file', 'files', or 'directory'")
            else:
                run_container(config)

            if config.output:
                print(f"Pushing output(s)")
                self.__push_output(config)
        except Exception as e:
            update_status(config, 2, f"'{config.identifier}' failed: {traceback.format_exc()}")
            raise e

        update_status(config, 1, f"Completed '{config.identifier}'")
