import os
from os import environ
from os.path import join

import pytest

from plantit_cli.run import Run
from plantit_cli.store.mock_store import MockStore
from plantit_cli.store.terrain_store import *

message = "Message!"
testdir = environ.get('TEST_DIRECTORY')


@pytest.fixture()
def remote_base_path():
    cyverse_username = os.environ.get('CYVERSE_USERNAME', None)

    if cyverse_username is None:
        raise ValueError("Missing environment variable 'CYVERSE_USERNAME'")

    return f"/iplant/home/{cyverse_username}"


token = None


class Token:
    __token = None

    @staticmethod
    def get():
        return Token.__token

    def __init__(self, token):
        Token.__token = token


@pytest.fixture()
def token():
    cyverse_username = os.environ.get('CYVERSE_USERNAME', None)
    cyverse_password = os.environ.get('CYVERSE_PASSWORD', None)

    if cyverse_username is None:
        raise ValueError("Missing environment variable 'CYVERSE_USERNAME'")
    if cyverse_password is None:
        raise ValueError("Missing environment variable 'CYVERSE_PASSWORD'")

    if Token.get() is None:
        response = requests.get(
            'https://de.cyverse.org/terrain/token/cas',
            auth=(cyverse_username, cyverse_password)).json()
        Token(response['access_token'])

    return Token.get()


@pytest.fixture
def file_name_1():
    return "test1.txt"


@pytest.fixture
def file_name_2():
    return "test2.txt"


@pytest.fixture
def terrain_store(token):
    return TerrainStore(token)


@pytest.fixture
def mock_store():
    return MockStore()


@pytest.fixture
def run_with_directory_input_and_file_output(remote_base_path, token):
    return Run(
        identifier='run_with_directory_input_and_file_output',
        workdir=testdir,
        image="docker://alpine:latest",
        command='ls $INPUT | tee $OUTPUT',
        input={
            'kind': 'directory',
            'from': join(remote_base_path, "testCollection"),
        },
        output={
            'to': join(remote_base_path, "testCollection"),
            'from': join(testdir, 'output.txt'),
        },
        cyverse_token=token)


@pytest.fixture
def run_with_directory_input_and_directory_output(remote_base_path, token):
    return Run(
        identifier='run_with_directory_input_and_directory_output',
        workdir=testdir,
        image="docker://alpine:latest",
        command='cp -r $INPUT $OUTPUT',
        input={
            'kind': 'directory',
            'from': join(remote_base_path, "testCollection"),
        },
        output={
            'to': join(remote_base_path, "testCollection"),
            'from': 'input',
        },
        cyverse_token=token)
