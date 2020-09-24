import os
import subprocess
from abc import ABC, abstractmethod
from os.path import join

from plantit_cli.exceptions import PlantitException
from plantit_cli.run import Run
from plantit_cli.store.terrain import TerrainStore
from plantit_cli.utils import update_status


class Executor(ABC):
    token = None

    @property
    @abstractmethod
    def name(self):
        pass

    @abstractmethod
    def execute(self, run: Run):
        pass

    def __store(self, path):
        return TerrainStore(path=path, token=self.token)

    def clone_repo(self, run: Run):
        ret = subprocess.run(f"git clone {run.clone}", stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             shell=True)
        if ret.returncode != 0:
            raise PlantitException(f"Failed to clone repository '{run.clone}'")
        else:
            update_status(run, 3, f"Cloned repository '{run.clone}'")

    def pull_input(self, run: Run) -> str:
        store = self.__store(run.input['from'])
        directory = join(run.workdir, 'input')
        os.makedirs(directory, exist_ok=True)
        store.pull(directory, run.input['pattern'] if 'pattern' in run.input else None)
        files = [os.path.abspath(join(directory, file)) for file in os.listdir(directory)]
        file_count = len(files)
        update_status(run, 3, f"Pulled {file_count} input(s) from '{run.input['from']}' to '{directory}': {files}")
        return directory

    def push_output(self, run: Run):
        store = self.__store(run.output['to'])
        local_path = join(run.workdir, run.output['from']) if 'from' in run.output else run.workdir
        store.push(local_path, run.output['pattern'] if 'pattern' in run.output else None)
        update_status(run, 3, f"Pushed output(s) from '{run.output['from']}' to '{run.output['to']}'")
