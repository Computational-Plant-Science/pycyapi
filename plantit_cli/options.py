from enum import Enum


class InputKind(str, Enum):
    FILE = 'file'
    FILES = 'files'
    DIRECTORY = 'directory'
