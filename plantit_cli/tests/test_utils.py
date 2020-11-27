from os import listdir, remove
from os.path import isdir, isfile, islink, join
import shutil


def clear_dir(dir):
    for file in listdir(dir):
        p = join(dir, file)
        if isfile(p) or islink(p):
            remove(p)
        elif isdir(p):
            shutil.rmtree(p)


def check_hello(file, name):
    assert isfile(file)
    with open(file) as file:
        lines = file.readlines()
        assert len(lines) == 1
        line = lines[0]
        assert f"Hello, {name}!" in line