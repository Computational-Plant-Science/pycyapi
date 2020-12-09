from os import listdir
from os.path import isfile, join


def list_files(path, pattern=None, exclude=None):
    all_paths = [join(path, file) for file in listdir(path) if isfile(join(path, file))]
    after_pattern = [p for p in all_paths if pattern.lower() in p.lower()] if pattern is not None else all_paths
    after_exclude = [p for p in after_pattern if p.split('/')[-1] not in exclude] if exclude is not None else after_pattern

    return after_exclude
