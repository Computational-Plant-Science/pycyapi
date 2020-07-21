import os
import subprocess
from abc import ABC, abstractmethod
from os.path import join

from plantit_cli.dagster.solids import update_status, construct_pipeline_with_input_files, construct_pipeline_with_input_directory
from plantit_cli.exceptions import PlantitException
from plantit_cli.run import Run
from plantit_cli.store.irodsstore import IRODSStore


class Executor(ABC):
    """
    Workflow execution engine.
    """

    irods_options = None

    @property
    @abstractmethod
    def name(self):
        pass

    @abstractmethod
    def execute(self, run: Run):
        pass

    def __irods(self, path):
        return IRODSStore(path=path) if self.irods_options is not None else IRODSStore(path=path, options=self.irods_options)

    def clone_repo_for(self, run: Run):
        ret = subprocess.run(f"git clone {run.clone}", stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             shell=True)
        if ret.returncode != 0:
            raise PlantitException(f"Failed to clone repository '{run.clone}'.")
        else:
            update_status(run, 3, f"Cloned repository '{run.clone}'.")

    def pull_inputs_for(self, run: Run):
        irods = self.__irods(run.input['irods_path'])
        local_directory = join(run.workdir, 'input')
        os.makedirs(local_directory, exist_ok=True)
        irods.pull(local_directory)
        files = [os.path.abspath(join(local_directory, file)) for file in os.listdir(local_directory)]
        file_count = len(files)
        update_status(run, 3, f"Pulled {file_count} input(s) from '{run.input['irods_path']}': {files}")

        if run.input['kind'] == 'file':
            dagster_pipeline = construct_pipeline_with_input_files(run, files)
            update_status(run, 3, f"Using fan-out workflow for {file_count} input file(s).")
        elif run.input['kind'] == 'directory':
            dagster_pipeline = construct_pipeline_with_input_directory(run, local_directory)
            update_status(run, 3, f"Using linear workflow for input directory '{local_directory}' containing {file_count} files.")
        else:
            raise ValueError(f"Value of 'input.kind' must be either 'file' or 'directory'")

        return dagster_pipeline

    def push_outputs_for(self, run: Run):
        irods = self.__irods(run.input['irods_path'])
        local_path = join(run.workdir, run.output['local_path'])
        irods.push(local_path)
        update_status(run, 3, f"Pushed output(s) to '{run.output['irods_path']}': {local_path}")