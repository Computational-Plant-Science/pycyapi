from enum import Enum
from typing import List, TypedDict


class BindMount(TypedDict):
    host_path: str
    container_path: str


class EnvironmentVariable(TypedDict):
    key: str
    value: str


class InputKind(str, Enum):
    FILE = 'file'
    FILES = 'files'
    DIRECTORY = 'directory'


class InputData(TypedDict, total=False):
    kind: InputKind
    path: str
    patterns: List[str]


class ScriptConfig(TypedDict, total=False):
    guid: str
    email: str
    workdir: str
    image: str
    command: str
    token: str
    input: InputData
    output: dict
    iterations: int
    env: List[EnvironmentVariable]
    bind_mounts: List[BindMount]
    log_file: str
    no_cache: bool
    gpus: bool
    shell: str
    walltime: str
    queue: str
    mem: str
    nodes: int
    cores: int
    tasks: int
    processes: int
    project: str
    launcher: bool
    job_array: bool
    header_skip: str
