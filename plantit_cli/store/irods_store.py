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
from contextlib import closing
from multiprocessing.dummy import Pool
from os.path import isdir, isfile, basename, join
from typing import List

import requests
from requests import ReadTimeout, Timeout, HTTPError, RequestException
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from irods.session import iRODSSession
from irods.models import Collection

from plantit_cli.utils import list_files


@retry(
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_attempt(3),
    retry=(retry_if_exception_type(ConnectionError) | retry_if_exception_type(
        RequestException) | retry_if_exception_type(ReadTimeout) | retry_if_exception_type(
        Timeout) | retry_if_exception_type(HTTPError)))
def dir_exists(path: str, session: iRODSSession) -> bool:
    results = session.query(Collection).filter(Collection.name == path).all()
    return len(results) > 0

@retry(
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_attempt(3),
    retry=(retry_if_exception_type(ConnectionError) | retry_if_exception_type(
        RequestException) | retry_if_exception_type(ReadTimeout) | retry_if_exception_type(
        Timeout) | retry_if_exception_type(HTTPError)))
def file_exists(path: str, session: iRODSSession) -> bool:
    pass


@retry(
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_attempt(3),
    retry=(retry_if_exception_type(ConnectionError) | retry_if_exception_type(
        RequestException) | retry_if_exception_type(ReadTimeout) | retry_if_exception_type(
        Timeout) | retry_if_exception_type(HTTPError)))
def list_dir(path: str, session: iRODSSession) -> List[str]:
    pass


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
    pass


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
        patterns: List[str],
        checksums: List[dict],
        session: iRODSSession,
        overwrite: bool = False):
    pass


@retry(
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_attempt(3),
    retry=(retry_if_exception_type(ConnectionError) | retry_if_exception_type(
        RequestException) | retry_if_exception_type(ReadTimeout) | retry_if_exception_type(
        Timeout) | retry_if_exception_type(HTTPError)))
def push_file(from_path: str, to_prefix: str, session: iRODSSession,):
    pass


def push_dir(
        from_path: str,
        to_prefix: str,
        include_patterns: List[str],
        include_names: List[str],
        exclude_patterns: List[str],
        exclude_names: List[str],
        session: iRODSSession,):
    pass