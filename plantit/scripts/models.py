from copy import deepcopy
from dataclasses import dataclass
from enum import Enum
from os import getcwd
from pprint import pformat
from typing import List, Optional


@dataclass
class BindMount:
    host_path: str
    container_path: str

    def __repr__(self):
        return (
            self.host_path + ":" + self.container_path
            if self.host_path != ""
            else self.container_path
        )

    def format(self, workdir: str) -> str:
        return (
            self.host_path + ":" + self.container_path
            if self.host_path != ""
            else workdir + ":" + self.container_path
        )

    @staticmethod
    def parse_bind_mount(s: str, workdir: str = getcwd()):
        split = s.rpartition(":")
        return (
            BindMount(host_path=split[0], container_path=split[2])
            if len(split) > 0
            else BindMount(host_path=workdir, container_path=s)
        )


@dataclass
class EnvironmentVariable:
    key: str
    value: str


class Shell(Enum):
    BASH = "bash"
    ZSH = "zsh"
    SH = "sh"


class ParallelStrategy(Enum):
    JOB_ARRAY = "job_array"
    LAUNCHER = "launcher"


@dataclass
class JobqueueConfig:
    queue: str
    nodes: int = 1
    cores: int = 1
    tasks: int = 1
    processes: int = 1
    mem: str = "1GB"
    walltime: str = "00:10:00"
    parallel: ParallelStrategy = ParallelStrategy.JOB_ARRAY
    project: Optional[str] = None
    header_skip: Optional[str] = None


@dataclass
class ScriptConfig:
    image: str
    entrypoint: str
    workdir: str
    email: str
    guid: str
    token: str
    jobqueue: JobqueueConfig
    shell: Shell = Shell.BASH
    source: Optional[str] = None
    sink: Optional[str] = None
    input: Optional[str] = None
    output: Optional[str] = None
    iterations: Optional[int] = None
    environment: Optional[List[EnvironmentVariable]] = None
    bind_mounts: Optional[List[BindMount]] = None
    no_cache: bool = False
    gpus: bool = False

    def __repr__(self):
        c = deepcopy(self)
        del c["token"]
        return pformat(c)
