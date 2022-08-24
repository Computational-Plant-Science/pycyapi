from os import getcwd
from copy import deepcopy
from dataclasses import dataclass
from pprint import pformat
from typing import List, Optional


@dataclass
class BindMount:
    host_path: str
    container_path: str

    def __repr__(self):
        return self.host_path + ':' + self.container_path \
            if self.host_path != '' \
            else self.container_path

    def format(self, workdir: str) -> str:
        return self.host_path + ':' + self.container_path \
            if self.host_path != '' \
            else workdir + ':' + self.container_path

    @staticmethod
    def parse_bind_mount(s: str, workdir: str = getcwd()):
        split = s.rpartition(':')
        return BindMount(host_path=split[0], container_path=split[2]) \
            if len(split) > 0 \
            else BindMount(host_path=workdir, container_path=s)


@dataclass
class EnvironmentVariable:
    key: str
    value: str


@dataclass
class JobqueueConfig:
    walltime: str
    queue: str
    project: str
    mem: str
    nodes: int
    cores: int
    tasks: int
    processes: int
    header_skip: Optional[str] = None


@dataclass
class SubmissionConfig:
    image: str
    command: str
    workdir: str
    email: str
    guid: str
    token: str
    jobqueue: JobqueueConfig
    job_array: bool = True
    launcher: bool = False
    shell: str = 'bash'
    source: Optional[str] = None
    sink: Optional[str] = None
    output: Optional[str] = None
    parallel: bool = False
    iterations: Optional[int] = None
    environment: Optional[List[EnvironmentVariable]] = None
    bind_mounts: Optional[List[BindMount]] = None
    log_file: Optional[str] = None
    no_cache: bool = False
    gpus: bool = False

    def __repr__(self):
        c = deepcopy(self)
        del c['token']
        return pformat(c)
