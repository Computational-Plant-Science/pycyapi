import os
import shutil
from os.path import isdir, isfile, islink


def clear_dir(dir):
    for file in os.listdir(dir):
        p = os.path.join(dir, file)
        if isfile(p) or islink(p):
            os.remove(p)
        elif isdir(p):
            shutil.rmtree(p)


def check_hello(file, name):
    assert isfile(file)
    with open(file) as file:
        lines = file.readlines()
        assert len(lines) == 1
        line = lines[0]
        assert f"Hello, {name}!" in line