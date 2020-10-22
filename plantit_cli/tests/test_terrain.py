import os
import tempfile
from os.path import join, isfile

from plantit_cli.collection.collection import Collection
from plantit_cli.collection.terrain import Terrain
from plantit_cli.run import Run
from plantit_cli.tests.utils import clear_dir, delete_collection, upload_file, create_collection

message = "Message!"
testdir = '/opt/plantit-cli/runs'
tempdir = tempfile.gettempdir()

def _run(remote_base_path, token):
    return Run(
        identifier='workflow_with_directory_input',
        workdir=testdir,
        image="docker://alpine:latest",
        command='ls $INPUT | tee $INPUT.output',
        input={
            'kind': 'directory',
            'from': join(remote_base_path, "testCollection"),
        },
        cyverse_token=token)


def test_list(remote_base_path, token):
    file1_name = 'f1.txt'
    file2_name = 'f2.txt'
    file1_path = join(testdir, file1_name)
    file2_path = join(testdir, file2_name)
    remote_path = join(remote_base_path, "testCollection")
    collection = Terrain(_run(remote_base_path, token))

    try:
        # prep CyVerse collection
        create_collection(remote_path, token)

        # create files
        with open(file1_path, "w") as file1, open(file2_path, "w") as file2:
            file1.write('Hello, 1!')
            file2.write('Hello, 2!')

        # upload files to CyVerse
        upload_file(file1_path, remote_path, token)
        upload_file(file2_path, remote_path, token)

        # list files
        files = collection.list()

        # check listed files
        assert join(remote_path, file1_name) in files
        assert join(remote_path, file2_name) in files
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)


def test_pull(remote_base_path, token):
    file1_name = 'f1.txt'
    file2_name = 'f2.txt'
    file1_path = join(testdir, file1_name)
    file2_path = join(testdir, file2_name)
    remote_path = join(remote_base_path, "testCollection")
    collection = Terrain(_run(remote_base_path, token))

    try:
        # prep CyVerse collection
        create_collection(remote_path, token)

        # create files
        with open(file1_path, "w") as file1, open(file2_path, "w") as file2:
            file1.write('Hello, 1!')
            file2.write('Hello, 2!')

        # upload files to CyVerse
        upload_file(file1_path, remote_path, token)
        upload_file(file2_path, remote_path, token)

        # remove files
        os.remove(file1_path)
        os.remove(file2_path)

        # pull files
        collection.pull(testdir, '.txt')

        # check files were pulled
        assert isfile(file1_path)
        assert isfile(file2_path)
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)