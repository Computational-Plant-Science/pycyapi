import os
import subprocess
import traceback
from abc import ABC
from os.path import join

from plantit_cli.exceptions import PlantitException
from plantit_cli.run import Run
from plantit_cli.collection.terrain import Terrain
from plantit_cli.utils import update_status, container_for_directory, containers_for_files, \
    container


class Executor(ABC):
    @staticmethod
    def __store(run: Run):
        return Terrain(run=run)

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
        Executor.__store(run).pull(input_dir, run.input['pattern'] if 'pattern' in run.input else None)

        update_status(run, 3, f"Pulled input(s)")
        return input_dir

    def __push_output(self, run: Run):
        self.__store(run).push(join(run.workdir, run.output['from']) if 'from' in run.output else run.workdir,
                               (run.output['pattern'] if run.output[
                                                             'pattern'] != '' else None) if 'pattern' in run.output else None)

        update_status(run, 3, f"Pushed output(s)")

    def execute(self, run: Run):
        update_status(run, 3, f"Starting '{run.identifier}'")

        try:
            if run.clone is not None and run.clone is not '':
                Executor.__clone_repo(run)

            if run.input:
                print(f"Pulling input(s)...")
                input_dir = self.__pull_input(run)
                if run.input['kind'].lower() == 'directory':
                    container_for_directory(run, input_dir)
                elif run.input['kind'].lower() == 'file':
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
