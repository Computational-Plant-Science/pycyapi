"""
These functions all require an active iRODS session with permissions for the specified resource. For instance, if using tickets, they must be preconfigured on the session state prior to invoking any method here, like:

```
from irods.session import Session
from irods.ticket import Ticket

# some stuff to get session s
# ...

Ticket(s, <ticket>).supply()
```
"""

import json
import multiprocessing
import traceback
from contextlib import closing
from multiprocessing.dummy import Pool
from os.path import isdir, isfile, basename, join
from typing import List

import requests
from irods.collection import iRODSCollection
from requests import ReadTimeout, Timeout, HTTPError, RequestException
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from irods.session import iRODSSession
from irods.models import Collection, DataObject
from irods.exception import CollectionDoesNotExist, DataObjectDoesNotExist, NoResultFound, CAT_SQL_ERR

from plantit_cli.utils import list_files, pattern_match


@retry(
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_attempt(3),
    retry=(retry_if_exception_type(ConnectionError) | retry_if_exception_type(
        RequestException) | retry_if_exception_type(ReadTimeout) | retry_if_exception_type(
        Timeout) | retry_if_exception_type(HTTPError)))
def dir_exists(path: str, session: iRODSSession) -> bool:
    try:
        session.collections.get(path)
        return True
    except (NoResultFound, CollectionDoesNotExist):
        return False

@retry(
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_attempt(3),
    retry=(retry_if_exception_type(ConnectionError) | retry_if_exception_type(
        RequestException) | retry_if_exception_type(ReadTimeout) | retry_if_exception_type(
        Timeout) | retry_if_exception_type(HTTPError)))
def file_exists(path: str, session: iRODSSession) -> bool:
    try:
        session.data_objects.get(path)
        return True
    except (NoResultFound, CollectionDoesNotExist, DataObjectDoesNotExist):
        return False


@retry(
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_attempt(3),
    retry=(retry_if_exception_type(ConnectionError) | retry_if_exception_type(
        RequestException) | retry_if_exception_type(ReadTimeout) | retry_if_exception_type(
        Timeout) | retry_if_exception_type(HTTPError)))
def list_dir(path: str, session: iRODSSession) -> List[str]:
    collection = session.collections.get(path)
    objects = collection.data_objects
    subcols = collection.subcollections
    return [col.path for col in subcols] + [obj.path for obj in objects]



@retry(
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_attempt(3),
    retry=(retry_if_exception_type(ConnectionError) | retry_if_exception_type(
        RequestException) | retry_if_exception_type(ReadTimeout) | retry_if_exception_type(
        Timeout) | retry_if_exception_type(HTTPError)))
def pull_file(
        from_path: str,
        to_path: str,
        session: iRODSSession,
        index: int = None,
        overwrite: bool = False):
    if isfile(to_path) and not overwrite:
        print(f"File {to_path} already exists, skipping download")
        return
    else:
        if index is None:
            print(f"Downloading file '{from_path}' to '{to_path}'")
        else:
            print(f"Downloading file '{from_path}' to '{to_path}' ({index})")

    try:
        session.data_objects.get(from_path, to_path)
    except CAT_SQL_ERR:
        return


@retry(
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_attempt(3),
    retry=(retry_if_exception_type(ConnectionError) | retry_if_exception_type(
        RequestException) | retry_if_exception_type(ReadTimeout) | retry_if_exception_type(
        Timeout) | retry_if_exception_type(HTTPError)))
def verify_checksums(from_path: str, expected_pairs: List[dict], session: iRODSSession):
    pass


def pull_dir(
        from_path: str,
        to_path: str,
        session: iRODSSession,
        patterns: List[str] = None,
        checksums: List[dict] = None,
        overwrite: bool = False):
    check = checksums is not None and len(checksums) > 0
    paths = list_dir(from_path, session)
    paths = [path for path in paths if pattern_match(path, patterns)] if (patterns is not None and len(patterns) > 0) else paths

    # verify  that input checksums haven't changed since submission time
    if check: verify_checksums(from_path, checksums)

    print(f"Downloading directory '{from_path}' with {len(paths)} file(s)")
    for i, path in enumerate(paths):
        pull_file(path, to_path, session, i)
    # TODO: some way to support parallelism? sessions can't be serialized
    # with closing(Pool(processes=multiprocessing.cpu_count())) as pool:
    #     args = [(path, to_path, session, i) for i, path in enumerate(paths)]
    #     pool.starmap(pull_file, args)

    # TODO: verify that input checksums haven't changed since download time?
    # (maybe a bit excessive, and will add network latency, but probably prudent)
    # if check: verify_checksums(from_path, checksums)


@retry(
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_attempt(3),
    retry=(retry_if_exception_type(ConnectionError) | retry_if_exception_type(
        RequestException) | retry_if_exception_type(ReadTimeout) | retry_if_exception_type(
        Timeout) | retry_if_exception_type(HTTPError)))
def push_file(from_path: str, to_prefix: str, session: iRODSSession):
    if isfile(to_prefix):
        print(f"File {to_prefix} already exists, skipping download")
        return
    else:
        print(f"Uploading file '{from_path}' to '{to_prefix}'")

    try:
        c_result = session.query(Collection).one()
        c = iRODSCollection(session.collections, c_result)

        session.data_objects.put(from_path, to_prefix)
    except CAT_SQL_ERR:
        return


def push_dir(
        from_path: str,
        to_prefix: str,
        session: iRODSSession,
        include_patterns: List[str] = None,
        include_names: List[str] = None,
        exclude_patterns: List[str] = None,
        exclude_names: List[str] = None):
    is_file = isfile(from_path)
    is_dir = isdir(from_path)

    if not (is_dir or is_file):
        raise FileNotFoundError(f"Local path '{from_path}' does not exist")
    elif is_dir:
        from_paths = list_files(from_path, include_patterns, include_names, exclude_patterns, exclude_names)
        print(f"Uploading directory '{from_path}' with {len(from_paths)} file(s) to '{to_prefix}'")
        for path in from_paths:
            push_file(path, to_prefix, session)
        # with closing(Pool(processes=multiprocessing.cpu_count())) as pool:
        #     args = [(path, to_prefix, token) for path in [str(p) for p in from_paths]]
        #     pool.starmap(push_file, args)
    elif is_file:
        push_file(from_path, to_prefix, session)
    else:
        raise ValueError(f"Remote path '{to_prefix}' is a file; specify a directory path instead")