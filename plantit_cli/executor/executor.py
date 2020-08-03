import os
import subprocess
from abc import ABC, abstractmethod
from os.path import join

import dask
from dask import distributed
from dask.distributed import Client

from plantit_cli.dagster.solids import update_status, construct_pipeline_with_input_files, \
    construct_pipeline_with_input_directory, execute_container
from plantit_cli.exceptions import PlantitException
from plantit_cli.run import Run
from plantit_cli.store.irods import IRODSStore


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
        return IRODSStore(path=path) if self.irods_options is None else IRODSStore(path=path, options=self.irods_options)

    def clone_repo(self, run: Run):
        ret = subprocess.run(f"git clone {run.clone}", stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             shell=True)
        if ret.returncode != 0:
            raise PlantitException(f"Failed to clone repository '{run.clone}'.")
        else:
            update_status(run, 3, f"Cloned repository '{run.clone}'.")

    # def pull_input(self, run: Run) -> [str]:
    #     irods = self.__irods(run.input['path'])
    #     local_directory = join(run.workdir, 'input')
    #     os.makedirs(local_directory, exist_ok=True)
    #     irods.pull(local_directory)
    #     files = [os.path.abspath(join(local_directory, file)) for file in os.listdir(local_directory)]
    #     file_count = len(files)
    #     update_status(run, 3, f"Pulled {file_count} input(s) from '{run.input['path']}': {files}")

    #     if run.input['kind'] == 'file':
    #         pipeline = construct_pipeline_with_input_files(run, files)
    #     elif run.input['kind'] == 'directory':
    #         pipeline = construct_pipeline_with_input_directory(run, local_directory)
    #     else:
    #         raise ValueError(f"Value of 'input.kind' must be either 'file' or 'directory'")

    #     return pipeline

    def pull_input(self, run: Run) -> [str]:
        irods = self.__irods(run.input['path'])
        directory = join(run.workdir, 'input')
        os.makedirs(directory, exist_ok=True)
        irods.pull(directory)
        files = [os.path.abspath(join(directory, file)) for file in os.listdir(directory)]
        file_count = len(files)
        update_status(run, 3, f"Pulled {file_count} input(s) from '{run.input['path']}': {files}")
        return directory

    def push_output(self, run: Run):
        irods = self.__irods(run.output['irods_path'])
        local_path = join(run.workdir, run.output['local_path']) if 'local_path' in run.output else run.workdir
        irods.push(local_path)
        update_status(run, 3, f"Pushed output(s) to '{run.output['irods_path']}': {local_path}")

    def execute_workflow_with_no_input(self, run: Run, client: Client):
        update_status(run, 3, f"Running '{run.image}' container.")
        if run.params:
            params = run.params.copy()
        else:
            params = []

        if run.output:
            output_path = join(run.workdir, run.output['local_path']) if run.output[
                                                                             'local_path'] is not '' else run.workdir
            params += [{'key': 'OUTPUT', 'value': output_path}]

        future = client.submit(execute_container, Run(
            identifier=run.identifier,
            token=run.token,
            api_url=run.api_url,
            workdir=run.workdir,
            image=run.image,
            command=run.command,
            params=params,
            input=run.input,
            output=run.output
        ))

        update_status(run, 2, future.result())

    def execute_workflow_with_directory_input(self, run: Run, client: Client, input_directory: str):
        if run.params:
            params = run.params.copy()
        else:
            params = []

        params += [{'key': 'INPUT', 'value': input_directory}]

        if run.output:
            output_path = join(run.workdir, run.output['local_path']) if run.output[
                                                                             'local_path'] is not '' else run.workdir
            params += [{'key': 'OUTPUT', 'value': output_path}]

        update_status(run, 3,
                      f"Running '{run.image}' container on input directory '{input_directory}'.")

        future = client.submit(execute_container, Run(
            identifier=run.identifier,
            token=run.token,
            api_url=run.api_url,
            workdir=run.workdir,
            image=run.image,
            command=run.command,
            params=params,
            input=run.input,
            output=run.output
        ))

        update_status(run, 2, future.result())

    def execute_workflow_with_file_input(self, run: Run, client: Client, input_directory: str):
        files = os.listdir(input_directory)
        futures = []

        update_status(run, 3,
                      f"Running {len(files)} '{run.image}' container(s) on files in input directory '{input_directory}'.")

        for file in files:
            if run.params:
                params = run.params.copy()
            else:
                params = []

            params += [{'key': 'INPUT', 'value': join(input_directory, file)}]

            output = {}
            if run.output:
                output = run.output.copy()
                params += [{'key': 'OUTPUT', 'value': join(run.workdir, output['local_path'])}]

            future = client.submit(execute_container, Run(
                identifier=run.identifier,
                token=run.token,
                api_url=run.api_url,
                workdir=run.workdir,
                image=run.image,
                command=run.command,
                params=params,
                input=run.input,
                output=output
            ))

            futures.append(future)

        for future in dask.distributed.as_completed(futures):
            update_status(run, 2, future.result())