import os
import tempfile
from os.path import join

import pytest

from plantit_cli.collection.terrain import *
from plantit_cli.executor.executor import Executor

message = "Message!"
testdir = '/opt/plantit-cli/runs/'
tempdir = tempfile.gettempdir()


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
def executor():
    return Executor()


def prep(definition):
    if 'api_url' not in definition:
        definition['api_url'] = None

    return definition


@pytest.fixture
def run_with_params():
    return Run(
        identifier='run_with_params',
        workdir=testdir,
        image="docker://alpine:latest",
        command='echo "$MESSAGE" >> $MESSAGE_FILE',
        params=[
            {
                'key': 'MESSAGE',
                'value': message
            },
            {
                'key': 'MESSAGE_FILE',
                'value': join(testdir, 'message.txt')
            },
        ])


@pytest.fixture
def run_with_file_input(remote_base_path, token):
    return Run(
        identifier='run_with_file_input',
        workdir=testdir,
        image="docker://alpine:latest",
        command='cat "$INPUT" | tee "$INPUT.output"',
        input={
            'kind': 'file',
            'pattern': '.txt',
            'from': join(remote_base_path, "testCollection"),
        },
        cyverse_token=token)


@pytest.fixture
def run_with_directory_input(remote_base_path, token):
    return Run(
        identifier='run_with_directory_input',
        workdir=testdir,
        image="docker://alpine:latest",
        command='ls $INPUT | tee $INPUT.output',
        input={
            'kind': 'directory',
            'from': join(remote_base_path, "testCollection"),
        },
        cyverse_token=token)


@pytest.fixture
def run_with_file_output(remote_base_path, token):
    return Run(
        identifier='run_with_file_output',
        workdir=testdir,
        image="docker://alpine:latest",
        command='echo "Hello, world!" >> $OUTPUT',
        output={
            'kind': 'file',
            'to': join(remote_base_path, "testCollection"),
            'from': 'output.txt',
        },
        cyverse_token=token)


@pytest.fixture
def run_with_directory_output(remote_base_path, token):
    return Run(
        identifier='run_with_directory_output',
        workdir=testdir,
        image="docker://alpine:latest",
        command='echo "Hello, world!" | tee $OUTPUT/t1.txt $OUTPUT/t2.txt',
        output={
            'kind': 'directory',
            'to': join(remote_base_path, "testCollection"),
            'from': '',
        },
        cyverse_token=token)


@pytest.fixture
def run_with_file_input_and_file_output(remote_base_path, token):
    return Run(
        identifier='run_with_file_input_and_file_output',
        workdir=testdir,
        image="docker://alpine:latest",
        command='cat $INPUT | tee $OUTPUT',
        input={
            'kind': 'file',
            'from': join(remote_base_path, "testCollection"),
        },
        output={
            'kind': 'file',
            'to': join(remote_base_path, "testCollection"),
            'from': join(testdir, 'output.txt'),
        },
        cyverse_token=token)


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
            'kind': 'file',
            'tp': join(remote_base_path, "testCollection"),
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
            'kind': 'directory',
            'to': join(remote_base_path, "testCollection"),
            'from': 'input',
        },
        cyverse_token=token)
