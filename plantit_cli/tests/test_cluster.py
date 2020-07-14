import os
import shutil
import tempfile
from os.path import join, isfile, islink, isdir

import pytest
from irods.session import iRODSSession

from plantit_cli.executor.inprocessexecutor import InProcessExecutor
from plantit_cli.run import Run

host = "irods"
port = 1247
user = "rods"
password = "rods"
zone = "tempZone"
path = f"/{zone}"
message = "Message!"
testdir = '/test'
tempdir = tempfile.gettempdir()


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


@pytest.fixture
def session():
    return iRODSSession(host=host,
                        port=port,
                        user=user,
                        password=password,
                        zone=zone)


@pytest.fixture
def executor():
    return InProcessExecutor()


@pytest.fixture
def workflow_with_params():
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


def test_workflow_with_params(executor, workflow_with_params):
    try:
        # run the workflow
        executor.execute(workflow_with_params)

        # check local message file
        file = join(testdir, 'message.txt')
        assert isfile(file)
        with open(file) as file:
            lines = file.readlines()
            assert len(lines) == 1
            assert lines[0] == f"{message}\n"
    finally:
        clear_dir(testdir)


@pytest.fixture
def workflow_with_file_input():
    return Run(
        identifier='workflow_with_file_input',
        api_url='',
        workdir=testdir,
        image="docker://alpine:latest",
        command='cat "$INPUT" | tee "$INPUT.output"',
        input={
            'kind': 'file',
            'host': host,
            'port': port,
            'username': user,
            'password': password,
            'zone': zone,
            'irods_path': join(path, "testCollection"),
        })


def test_workflow_with_file_input(session, executor, workflow_with_file_input):
    local_file_1 = tempfile.NamedTemporaryFile()
    local_file_2 = tempfile.NamedTemporaryFile()
    local_file_1_name = local_file_1.name.split('/')[-1]
    local_file_2_name = local_file_2.name.split('/')[-1]
    collection = join(path, "testCollection")

    try:
        # prep iRODS files
        session.collections.create(collection)
        local_file_1.write(b'Hello, 1!')
        local_file_1.seek(0)
        local_file_2.write(b'Hello, 2!')
        local_file_2.seek(0)
        session.data_objects.put(local_file_1.name, join(collection, local_file_1_name))
        session.data_objects.put(local_file_2.name, join(collection, local_file_2_name))
        local_file_1.close()
        local_file_2.close()

        # run the workflow (expect 2 containers, 1 for each input file)
        executor.execute(workflow_with_file_input)

        # check input files were pulled from iRODS
        input_1 = join(testdir, 'input', local_file_1_name)
        input_2 = join(testdir, 'input', local_file_2_name)
        check_hello(input_1, 1)
        check_hello(input_2, 2)
        os.remove(input_1)
        os.remove(input_2)

        # check local output files were written
        output_1 = f"{input_1}.output"
        output_2 = f"{input_2}.output"
        check_hello(output_1, 1)
        check_hello(output_2, 2)
        os.remove(output_1)
        os.remove(output_2)
    finally:
        clear_dir(testdir)
        session.collections.remove(collection, force=True)


@pytest.fixture
def workflow_with_directory_input():
    return Run(
        identifier='workflow_with_directory_input',
        api_url='',
        workdir=testdir,
        image="docker://alpine:latest",
        command='ls $INPUT | tee output.txt',
        input={
            'kind': 'directory',
            'host': host,
            'port': port,
            'username': user,
            'password': password,
            'zone': zone,
            'irods_path': join(path, "testCollection"),
        })


def test_workflow_with_directory_input(session, executor, workflow_with_directory_input):
    local_file_1 = tempfile.NamedTemporaryFile()
    local_file_2 = tempfile.NamedTemporaryFile()
    local_path_1 = local_file_1.name
    local_path_2 = local_file_2.name
    local_name_1 = local_file_1.name.split('/')[-1]
    local_name_2 = local_file_2.name.split('/')[-1]
    collection = join(path, "testCollection")
    remote_path_1 = join(collection, local_name_1)
    remote_path_2 = join(collection, local_name_2)

    try:
        # prep iRODS collection
        session.collections.create(collection)
        local_file_1.write(b'Hello, 1!')
        local_file_1.seek(0)
        local_file_2.write(b'Hello, 2!')
        local_file_2.seek(0)
        session.data_objects.put(local_path_1, remote_path_1)
        session.data_objects.put(local_path_2, remote_path_2)
        local_file_1.close()
        local_file_2.close()

        # run the workflow (expect 1 container)
        executor.execute(workflow_with_directory_input)

        # check input files were pulled from iRODS
        input_1 = join(testdir, 'input', local_name_1)
        input_2 = join(testdir, 'input', local_name_2)
        check_hello(input_1, 1)
        check_hello(input_2, 2)
        os.remove(input_1)
        os.remove(input_2)

        # check local output files were written
        output = f"output.txt"
        assert isfile(output)
        with open(output, 'r') as file:
            lines = [line.strip('\n') for line in file.readlines()]
            assert len(lines) == 2
            assert local_name_1 in lines
            assert local_name_2 in lines
        os.remove(output)
    finally:
        clear_dir(testdir)
        session.collections.remove(collection, force=True)


@pytest.fixture
def workflow_with_file_output():
    return Run(
        identifier='workflow_with_file_output',
        api_url='',
        workdir=testdir,
        image="docker://alpine:latest",
        command='echo "Hello, world!" >> $OUTPUT',
        output={
            'kind': 'file',
            'host': host,
            'port': port,
            'username': user,
            'password': password,
            'zone': zone,
            'irods_path': join(path, "testCollection"),
            'local_path': 'output.txt',
        })


def test_workflow_with_file_output(session, executor, workflow_with_file_output):
    local_path = join(testdir, workflow_with_file_output.output['local_path'])
    collection = join(path, "testCollection")

    try:
        # prep iRODS collection
        session.collections.create(collection)

        # run the workflow
        executor.execute(workflow_with_file_output)

        # check file was written
        assert isfile(local_path)
        check_hello(local_path, 'world')
        os.remove(local_path)

        # check file was pushed to iRODS
        session.data_objects.get(join(collection, 'output.txt'), local_path)
        check_hello(local_path, 'world')
        os.remove(local_path)
    finally:
        clear_dir(testdir)
        session.collections.remove(collection, force=True)


@pytest.fixture
def workflow_with_directory_output():
    return Run(
        identifier='workflow_with_directory_output',
        api_url='',
        workdir=testdir,
        image="docker://alpine:latest",
        command='echo "Hello, world!" | tee $OUTPUT/t1.txt $OUTPUT/t2.txt',
        output={
            'kind': 'directory',
            'host': host,
            'port': port,
            'username': user,
            'password': password,
            'zone': zone,
            'irods_path': join(path, "testCollection"),
            'local_path': '',
        })


def test_workflow_with_directory_output(session, executor, workflow_with_directory_output):
    local_path = join(testdir, workflow_with_directory_output.output['local_path'])
    output_1_path = join(local_path, 't1.txt')
    output_2_path = join(local_path, 't2.txt')
    collection = join(path, "testCollection")

    try:
        # prep iRODS collection
        session.collections.create(collection)

        # execute the workflow
        executor.execute(workflow_with_directory_output)

        # check files were written
        assert isfile(output_1_path)
        assert isfile(output_2_path)
        check_hello(output_1_path, 'world')
        check_hello(output_2_path, 'world')
        os.remove(output_1_path)
        os.remove(output_2_path)

        # check files were pushed to iRODS
        session.data_objects.get(join(collection, 't1.txt'), output_1_path)
        session.data_objects.get(join(collection, 't2.txt'), output_2_path)
        assert isfile(output_1_path)
        assert isfile(output_2_path)
        check_hello(output_1_path, 'world')
        check_hello(output_2_path, 'world')
        os.remove(output_1_path)
        os.remove(output_2_path)
    finally:
        clear_dir(testdir)
        session.collections.remove(collection, force=True)


@pytest.fixture
def workflow_with_file_input_and_file_output():
    return Run(
        identifier='workflow_with_file_input_and_file_output',
        api_url='',
        workdir=testdir,
        image="docker://alpine:latest",
        command='cat $INPUT | tee $OUTPUT',
        input={
            'kind': 'file',
            'host': host,
            'port': port,
            'username': user,
            'password': password,
            'zone': zone,
            'irods_path': join(path, "testCollection"),
        },
        output={
            'kind': 'file',
            'host': host,
            'port': port,
            'username': user,
            'password': password,
            'zone': zone,
            'irods_path': join(path, "testCollection"),
            'local_path': join(testdir, 'output.txt'),
        }
    )


# TODO


@pytest.fixture
def workflow_with_directory_input_and_file_output():
    return Run(
        identifier='workflow_with_directory_input_and_file_output',
        api_url='',
        workdir=testdir,
        image="docker://alpine:latest",
        command='ls $INPUT | tee $OUTPUT',
        input={
            'kind': 'directory',
            'host': host,
            'port': port,
            'username': user,
            'password': password,
            'zone': zone,
            'irods_path': join(path, "testCollection"),
        },
        output={
            'kind': 'file',
            'host': host,
            'port': port,
            'username': user,
            'password': password,
            'zone': zone,
            'irods_path': join(path, "testCollection"),
            'local_path': join(testdir, 'output.txt'),
        }
    )


# TODO


@pytest.fixture
def workflow_with_directory_input_and_directory_output():
    return Run(
        identifier='workflow_with_directory_input_and_directory_output',
        api_url='',
        workdir=testdir,
        image="docker://alpine:latest",
        command='cp -r $INPUT $OUTPUT',
        input={
            'kind': 'directory',
            'host': host,
            'port': port,
            'username': user,
            'password': password,
            'zone': zone,
            'irods_path': join(path, "testCollection"),
        },
        output={
            'kind': 'directory',
            'host': host,
            'port': port,
            'username': user,
            'password': password,
            'zone': zone,
            'irods_path': join(path, "testCollection"),
            'local_path': 'input',
        })

