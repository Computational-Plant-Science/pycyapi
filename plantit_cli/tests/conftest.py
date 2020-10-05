import os
import tempfile
from os.path import join

import pytest

from plantit_cli.executor.local import InProcessExecutor
from plantit_cli.run import Run
from plantit_cli.collection.terrain import *

message = "Message!"
testdir = '/test'
tempdir = tempfile.gettempdir()


@pytest.fixture()
def remote_base_path():
    return f"/iplant/home/{os.environ.get('CYVERSE_USERNAME')}"


@pytest.fixture()
def token(remote_base_path):
    return requests.get(
        'https://de.cyverse.org/terrain/token/cas',
        auth=(os.environ.get('CYVERSE_USERNAME'), os.environ.get('CYVERSE_PASSWORD'))).json()['access_token']


@pytest.fixture
def executor(token):
    return InProcessExecutor(token)


def prep(definition):
    if 'executor' in definition:
        del definition['executor']

    if 'api_url' not in definition:
        definition['api_url'] = None

    return definition


@pytest.fixture
def workflow_with_params():
    # definition = yaml.safe_load('examples/workflow_with_params.yaml')
    # definition = prep(definition)
    # return Run(**definition)
    return Run(
        identifier='workflow_with_params',
        api_url='',
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
def workflow_with_file_input(remote_base_path):
    return Run(
        identifier='workflow_with_file_input',
        workdir=testdir,
        image="docker://alpine:latest",
        command='cat "$INPUT" | tee "$INPUT.output"',
        input={
            'kind': 'file',
            'pattern': '.txt',
            'from': join(remote_base_path, "testCollection"),
        })


@pytest.fixture
def workflow_with_directory_input(remote_base_path):
    return Run(
        identifier='workflow_with_directory_input',
        workdir=testdir,
        image="docker://alpine:latest",
        command='ls $INPUT | tee $INPUT.output',
        input={
            'kind': 'directory',
            'from': join(remote_base_path, "testCollection"),
        })


@pytest.fixture
def workflow_with_file_output(remote_base_path):
    return Run(
        identifier='workflow_with_file_output',
        workdir=testdir,
        image="docker://alpine:latest",
        command='echo "Hello, world!" >> $OUTPUT',
        output={
            'kind': 'file',
            'to': join(remote_base_path, "testCollection"),
            'from': 'output.txt',
        })


@pytest.fixture
def workflow_with_directory_output(remote_base_path):
    return Run(
        identifier='workflow_with_directory_output',
        workdir=testdir,
        image="docker://alpine:latest",
        command='echo "Hello, world!" | tee $OUTPUT/t1.txt $OUTPUT/t2.txt',
        output={
            'kind': 'directory',
            'to': join(remote_base_path, "testCollection"),
            'from': '',
        })


@pytest.fixture
def workflow_with_file_input_and_file_output(remote_base_path):
    return Run(
        identifier='workflow_with_file_input_and_file_output',
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
        })


@pytest.fixture
def workflow_with_directory_input_and_file_output(remote_base_path):
    return Run(
        identifier='workflow_with_directory_input_and_file_output',
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
        })


@pytest.fixture
def workflow_with_directory_input_and_directory_output(remote_base_path):
    return Run(
        identifier='workflow_with_directory_input_and_directory_output',
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
        })
