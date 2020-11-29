import os
from os import environ

import pytest

from plantit_cli.store.terrain_store import *
from plantit_cli.tests.test_utils import get_token

message = "Message!"
testdir = environ.get('TEST_DIRECTORY')


@pytest.fixture()
def remote_base_path():
    cyverse_username = os.environ.get('CYVERSE_USERNAME', None)

    if cyverse_username is None:
        raise ValueError("Missing environment variable 'CYVERSE_USERNAME'")

    return f"/iplant/home/{cyverse_username}"


@pytest.fixture
def file_name_1():
    return "test1.txt"


@pytest.fixture
def file_name_2():
    return "test2.txt"


@pytest.fixture
def terrain_store():
    return TerrainStore(get_token())
