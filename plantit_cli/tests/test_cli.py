#import os
#import tempfile
#from os.path import join, isfile
#
#from click.testing import CliRunner
#
#from plantit_cli.cli import run
#from plantit_cli.tests.utils import clear_dir, check_hello
#
#zone = "tempZone"
#path = f"/{zone}"
#message = "Hello, world!"
#testdir = '/test'
#tempdir = tempfile.gettempdir()
#runner = CliRunner()
#
#
#def test_workflow_with_params():
#    try:
#        # run the workflow
#        os.environ['LC_ALL'] = 'C.UTF-8'
#        os.environ['LANG'] = 'C.UTF-8'
#        result = runner.invoke(run, ['examples/workflow_with_params.yaml'])
#        assert result.exit_code == 0
#
#        # check local message file
#        file = join(testdir, 'message.txt')
#        assert isfile(file)
#        with open(file) as file:
#            lines = file.readlines()
#            assert len(lines) == 1
#            assert lines[0] == f"{message}\n"
#    finally:
#        clear_dir(testdir)
#
#
## def test_workflow_with_params_slurm():
##     try:
##         # run the workflow
##         os.environ['LC_ALL'] = 'C.UTF-8'
##         os.environ['LANG'] = 'C.UTF-8'
##         result = runner.invoke(run, ['examples/workflow_with_params_slurm.yaml'])
##         assert result.exit_code == 0
##
##         # check local message file
##         file = join(testdir, 'message.txt')
##         assert isfile(file)
##         with open(file) as file:
##             lines = file.readlines()
##             assert len(lines) == 1
##             assert lines[0] == f"{message}\n"
##     finally:
##         clear_dir(testdir)
#
#
#def test_workflow_with_file_input(session):
#    local_file_1 = tempfile.NamedTemporaryFile()
#    local_file_2 = tempfile.NamedTemporaryFile()
#    local_file_1_name = local_file_1.name.split('/')[-1]
#    local_file_2_name = local_file_2.name.split('/')[-1]
#    collection = join(path, "testCollection")
#
#    try:
#        # prep iRODS files
#        session.collections.create(collection)
#        local_file_1.write(b'Hello, 1!')
#        local_file_1.seek(0)
#        local_file_2.write(b'Hello, 2!')
#        local_file_2.seek(0)
#        session.data_objects.put(local_file_1.name, join(collection, local_file_1_name))
#        session.data_objects.put(local_file_2.name, join(collection, local_file_2_name))
#        local_file_1.close()
#        local_file_2.close()
#
#        # run the workflow (expect 2 containers, 1 for each input file)
#        os.environ['LC_ALL'] = 'C.UTF-8'
#        os.environ['LANG'] = 'C.UTF-8'
#        result = runner.invoke(run, ['examples/workflow_with_file_input.yaml',
#                                     '--irods_host', 'irods',
#                                     '--irods_port', '1247',
#                                     '--irods_username', 'rods',
#                                     '--irods_password', 'rods',
#                                     '--irods_zone', 'tempZone'])
#        assert result.exit_code == 0
#
#        # check input files were pulled from iRODS
#        input_1 = join(testdir, 'input', local_file_1_name)
#        input_2 = join(testdir, 'input', local_file_2_name)
#        check_hello(input_1, 1)
#        check_hello(input_2, 2)
#        os.remove(input_1)
#        os.remove(input_2)
#
#        # check local output files were written
#        output_1 = f"{input_1}.output"
#        output_2 = f"{input_2}.output"
#        check_hello(output_1, 1)
#        check_hello(output_2, 2)
#        os.remove(output_1)
#        os.remove(output_2)
#    finally:
#        clear_dir(testdir)
#        session.collections.remove(collection, force=True)
#
#
#def test_workflow_with_directory_input(session):
#    local_file_1 = tempfile.NamedTemporaryFile()
#    local_file_2 = tempfile.NamedTemporaryFile()
#    local_path_1 = local_file_1.name
#    local_path_2 = local_file_2.name
#    local_name_1 = local_file_1.name.split('/')[-1]
#    local_name_2 = local_file_2.name.split('/')[-1]
#    collection = join(path, "testCollection")
#    remote_path_1 = join(collection, local_name_1)
#    remote_path_2 = join(collection, local_name_2)
#
#    try:
#        # prep iRODS collection
#        session.collections.create(collection)
#        local_file_1.write(b'Hello, 1!')
#        local_file_1.seek(0)
#        local_file_2.write(b'Hello, 2!')
#        local_file_2.seek(0)
#        session.data_objects.put(local_path_1, remote_path_1)
#        session.data_objects.put(local_path_2, remote_path_2)
#        local_file_1.close()
#        local_file_2.close()
#
#        # run the workflow (expect 1 container)
#        os.environ['LC_ALL'] = 'C.UTF-8'
#        os.environ['LANG'] = 'C.UTF-8'
#        result = runner.invoke(run, ['examples/workflow_with_directory_input.yaml',
#                                     '--irods_host', 'irods',
#                                     '--irods_port', '1247',
#                                     '--irods_username', 'rods',
#                                     '--irods_password', 'rods',
#                                     '--irods_zone', 'tempZone'])
#        assert result.exit_code == 0
#
#        # check input files were pulled from iRODS
#        input_1 = join(testdir, 'input', local_name_1)
#        input_2 = join(testdir, 'input', local_name_2)
#        check_hello(input_1, 1)
#        check_hello(input_2, 2)
#        os.remove(input_1)
#        os.remove(input_2)
#
#        # check local output files were written
#        output = f"output.txt"
#        assert isfile(output)
#        with open(output, 'r') as file:
#            lines = [line.strip('\n') for line in file.readlines()]
#            assert len(lines) == 2
#            assert local_name_1 in lines
#            assert local_name_2 in lines
#        os.remove(output)
#    finally:
#        clear_dir(testdir)
#        session.collections.remove(collection, force=True)
#
#
#def test_workflow_with_file_output(session, workflow_with_file_output):
#    path = join(testdir, workflow_with_file_output.output['path'])
#    collection = join(path, "testCollection")
#
#    try:
#        # prep iRODS collection
#        session.collections.create(collection)
#
#        # run the workflow
#        os.environ['LC_ALL'] = 'C.UTF-8'
#        os.environ['LANG'] = 'C.UTF-8'
#        result = runner.invoke(run, ['examples/workflow_with_file_output.yaml',
#                                     '--irods_host', 'irods',
#                                     '--irods_port', '1247',
#                                     '--irods_username', 'rods',
#                                     '--irods_password', 'rods',
#                                     '--irods_zone', 'tempZone'])
#        assert result.exit_code == 0
#
#        # check file was written
#        assert isfile(path)
#        check_hello(path, 'world')
#        os.remove(path)
#
#        # check file was pushed to iRODS
#        session.data_objects.get(join(collection, 'output.txt'), path)
#        check_hello(path, 'world')
#        os.remove(path)
#    finally:
#        clear_dir(testdir)
#        session.collections.remove(collection, force=True)
#
#
#def test_workflow_with_directory_output(session, workflow_with_directory_output):
#    path = join(testdir, workflow_with_directory_output.output['path'])
#    output_1_path = join(path, 't1.txt')
#    output_2_path = join(path, 't2.txt')
#    collection = join(path, "testCollection")
#
#    try:
#        # prep iRODS collection
#        session.collections.create(collection)
#
#        # execute the workflow
#        os.environ['LC_ALL'] = 'C.UTF-8'
#        os.environ['LANG'] = 'C.UTF-8'
#        result = runner.invoke(run, ['examples/workflow_with_directory_output.yaml',
#                                     '--irods_host', 'irods',
#                                     '--irods_port', '1247',
#                                     '--irods_username', 'rods',
#                                     '--irods_password', 'rods',
#                                     '--irods_zone', 'tempZone'])
#        assert result.exit_code == 0
#
#        # check files were written
#        assert isfile(output_1_path)
#        assert isfile(output_2_path)
#        check_hello(output_1_path, 'world')
#        check_hello(output_2_path, 'world')
#        os.remove(output_1_path)
#        os.remove(output_2_path)
#
#        # check files were pushed to iRODS
#        session.data_objects.get(join(collection, 't1.txt'), output_1_path)
#        session.data_objects.get(join(collection, 't2.txt'), output_2_path)
#        assert isfile(output_1_path)
#        assert isfile(output_2_path)
#        check_hello(output_1_path, 'world')
#        check_hello(output_2_path, 'world')
#        os.remove(output_1_path)
#        os.remove(output_2_path)
#    finally:
#        clear_dir(testdir)
#        session.collections.remove(collection, force=True)
#