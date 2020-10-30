import os
import subprocess
import traceback
from abc import ABC
from os.path import join

from plantit_cli.exceptions import PlantitException
from plantit_cli.run import Run
from plantit_cli.collection.terrain import Terrain
from plantit_cli.utils import update_status, run_container_directory_input, run_container_file_input, \
    run_container_no_input


class Executor(ABC):
    def __store(self, run: Run):
        return Terrain(run=run)

    def __clone_repo(self, run: Run):
        if subprocess.run(f"git clone {run.clone}",
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE,
                             shell=True).returncode != 0:
            raise PlantitException(f"Failed to clone repository '{run.clone}'")
        else:
            update_status(run, 3, f"Cloned repository '{run.clone}'")

    def __pull_input(self, run: Run) -> str:
        store = self.__store(run)
        directory = join(run.workdir, 'input')
        os.makedirs(directory, exist_ok=True)
        store.pull(directory, run.input['pattern'] if 'pattern' in run.input else None)
        update_status(run, 3, f"Pulled input(s)")
        return directory

    def __push_output(self, run: Run):
        store = self.__store(run)
        local_path = join(run.workdir, run.output['from']) if 'from' in run.output else run.workdir
        store.push(local_path, (run.output['pattern'] if run.output['pattern'] != '' else None) if 'pattern' in run.output else None)
        update_status(run, 3, f"Pushed output(s)")

    def execute(self, run: Run):
        update_status(run, 3, f"Starting '{run.identifier}'...")
        try:
            if run.clone is not None and run.clone is not '':
                self.__clone_repo(run)

            if run.input:
                print(f"Pulling inputs for '{run.identifier}'...")
                input_directory = self.__pull_input(run)
                if run.input['kind'].lower() == 'directory':
                    run_container_directory_input(run, input_directory)
                elif run.input['kind'].lower() == 'file':
                    run_container_file_input(run, input_directory)
                else:
                    raise ValueError(f"'input.kind' must be either 'file' or 'directory'")
            else:
                run_container_no_input(run)

            if run.output:
                print(f"Pushing outputs for '{run.identifier}'...")
                self.__push_output(run)

            update_status(run, 2, f"Run '{run.identifier}' completed.")
        except Exception:
            update_status(run, 3, f"Run '{run.identifier}' failed: {traceback.format_exc()}")
            return