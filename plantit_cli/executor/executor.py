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

    @property
    @abstractmethod
    def name(self):
        pass

    @abstractmethod
    def execute(self, run: Run):
        pass

    @staticmethod
    def clone(run: Run):
        ret = subprocess.run(f"git clone {run.clone}", stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             shell=True)
        if ret.returncode != 0:
            raise PlantitException(f"Failed to clone repository '{run.clone}'.")
        else:
            update_status(run, 3, f"Cloned repository '{run.clone}'.")

    @staticmethod
    def input(run: Run):
        irods = IRODSStore(
            path=run.input['irods_path'],
            host=run.input['host'],
            port=run.input['port'],
            user=run.input['username'],
            password=run.input['password'],
            zone=run.input['zone'],
        )

        directory = join(run.workdir, 'input')
        os.makedirs(directory, exist_ok=True)
        irods.pull(directory)
        files = [os.path.abspath(join(directory, file)) for file in os.listdir(directory)]
        file_count = len(files)
        update_status(run, 3, f"Pulled {file_count} input(s) from '{run.input['irods_path']}': {files}")

        if run.input['kind'] == 'file':
            dagster_pipeline = construct_pipeline_with_input_files(run, files)
            update_status(run, 3, f"Using fan-out workflow for {file_count} input file(s).")
        elif run.input['kind'] == 'directory':
            dagster_pipeline = construct_pipeline_with_input_directory(run, directory)
            update_status(run, 3, f"Using linear workflow for input directory '{directory}' containing {file_count} files.")
        else:
            raise ValueError(f"Value of 'input.kind' must be either 'file' or 'directory'")

        return dagster_pipeline

    @staticmethod
    def output(run: Run):
        irods = IRODSStore(
            path=run.output['irods_path'],
            host=run.output['host'],
            port=run.output['port'],
            user=run.output['username'],
            password=run.output['password'],
            zone=run.output['zone'],
        )

        path = join(run.workdir, run.output['local_path'])
        irods.push(path)
        update_status(run, 3, f"Pushed output(s) to '{run.output['irods_path']}': {path}")