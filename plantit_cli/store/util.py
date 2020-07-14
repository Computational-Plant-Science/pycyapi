from os import listdir
from os.path import isfile, join


def list_files(path):
    return [join(path, file) for file in listdir(path) if isfile(join(path, file))]