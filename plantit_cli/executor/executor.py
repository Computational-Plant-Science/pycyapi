import os
import subprocess
from abc import ABC, abstractmethod
from os.path import join

from plantit_cli.exceptions import PlantitException
from plantit_cli.run import Run
from plantit_cli.store.irods import IRODSStore
from plantit_cli.utils import update_status


class Executor(ABC):
    irods_options = None

    @property
    @abstractmethod
    def name(self):
        pass

    @abstractmethod
    def execute(self, run: Run):
        pass

    def __irods(self, path):
        return IRODSStore(path=path) if self.irods_options is None else IRODSStore(path=path,
                                                                                   options=self.irods_options)

    def clone_repo(self, run: Run):
        ret = subprocess.run(f"git clone {run.clone}", stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             shell=True)
        if ret.returncode != 0:
            raise PlantitException(f"Failed to clone repository '{run.clone}'.")
        else:
            update_status(run, 3, f"Cloned repository '{run.clone}'.")

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
