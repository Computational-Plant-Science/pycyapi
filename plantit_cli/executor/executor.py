import os
import subprocess
import traceback
from abc import ABC
from os.path import join

from plantit_cli.exceptions import PlantitException
from plantit_cli.run import Run
from plantit_cli.store.store import Store
from plantit_cli.utils import update_status, container_for_directory, containers_for_files, \
    container


class Executor(ABC):

    def __init__(self, store: Store):
        self.__store = store

    @staticmethod
    def __clone_repo(run: Run):
        if subprocess.run(f"git clone {run.clone}",
                          stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE,
                          shell=True).returncode != 0:
            raise PlantitException(f"Failed to clone repo '{run.clone}'")
        else:
            update_status(run, 3, f"Cloned repo '{run.clone}'")

    def __pull_input(self, run: Run) -> str:
        input_dir = join(run.workdir, 'input')
        os.makedirs(input_dir, exist_ok=True)

        if run.input['kind'].lower() == 'file':
            self.__store.download_file(run.input['from'], input_dir)
        elif run.input['kind'].lower() == 'directory':
            self.__store.download_directory(run.input['from'], input_dir,
                                     run.input['pattern'] if 'pattern' in run.input else None)
        else:
            raise ValueError(f"'input.kind' must be either 'file' or 'directory'")

        update_status(run, 3, f"Pulled input(s)")
        return input_dir

    def __push_output(self, run: Run):
        self.__store.upload_directory(join(run.workdir, run.output['from']) if 'from' in run.output else run.workdir,
                               run.output['to'],
                               (run.output['pattern'] if run.output[
                                                             'pattern'] != '' else None) if 'pattern' in run.output else None,
                               (run.output['exclude'] if run.output[
                                                             'exclude'] != '' else None) if 'exclude' in run.output else None)

        update_status(run, 3, f"Pushed output(s)")

    def execute(self, run: Run):
        update_status(run, 3, f"Starting '{run.identifier}'")

        try:
            if run.clone is not None and run.clone != '':
                Executor.__clone_repo(run)

            if run.input:
                print(f"Pulling input(s)...")
                input_dir = self.__pull_input(run)
                if run.input['kind'].lower() == 'directory' and 'many' not in run.input:
                    container_for_directory(run, input_dir)
                elif (run.input['kind'].lower() == 'directory' and run.input['many']) or run.input['kind'].lower() == 'file':
                    containers_for_files(run, input_dir)
                else:
                    raise ValueError(f"'input.kind' must be either 'file' or 'directory'")
            else:
                container(run)

            if run.output:
                print(f"Pushing output(s)...")
                self.__push_output(run)
        except Exception:
            update_status(run, 2, f"'{run.identifier}' failed: {traceback.format_exc()}")
            return

        update_status(run, 1, f"Completed '{run.identifier}'")
