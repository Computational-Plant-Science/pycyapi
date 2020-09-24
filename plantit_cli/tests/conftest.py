#import tempfile
#from os.from_path import join
#
#import pytest
#from irods.session import iRODSSession
#
#from plantit_cli.executor.local import InProcessExecutor
#from plantit_cli.run import Run
#from plantit_cli.store.irods import IRODSOptions
#
#host = "irods"
#port = 1247
#user = "rods"
#password = "rods"
#zone = "tempZone"
#from_path = f"/{zone}"
#message = "Message!"
#testdir = '/test'
#tempdir = tempfile.gettempdir()
#
#
#@pytest.fixture
#def session():
#    return iRODSSession(host=host,
#                        port=port,
#                        user=user,
#                        password=password,
#                        zone=zone)
#
#
#@pytest.fixture
#def executor():
#    return InProcessExecutor(IRODSOptions(host=host,
#                                          port=port,
#                                          username=user,
#                                          password=password,
#                                          zone=zone))
#
#
#def prep(definition):
#    if 'executor' in definition:
#        del definition['executor']
#
#    if 'api_url' not in definition:
#        definition['api_url'] = None
#
#    return definition
#
#
#@pytest.fixture
#def workflow_with_params():
#    # definition = yaml.safe_load('examples/workflow_with_params.yaml')
#    # definition = prep(definition)
#    # return Run(**definition)
#    return Run(
#        identifier='workflow_with_params',
#        api_url='',
#        workdir=testdir,
#        image="docker://alpine:latest",
#        command='echo "$MESSAGE" >> $MESSAGE_FILE',
#        params=[
#            {
#                'key': 'MESSAGE',
#                'value': message
#            },
#            {
#                'key': 'MESSAGE_FILE',
#                'value': join(testdir, 'message.txt')
#            },
#        ])
#
#
#@pytest.fixture
#def workflow_with_file_input():
#    return Run(
#        identifier='workflow_with_file_input',
#        api_url='',
#        workdir=testdir,
#        image="docker://alpine:latest",
#        command='cat "$INPUT" | tee "$INPUT.output"',
#        input={
#            'kind': 'file',
#            'from_path': join(from_path, "testCollection"),
#        })
#
#
#@pytest.fixture
#def workflow_with_directory_input():
#    return Run(
#        identifier='workflow_with_directory_input',
#        api_url='',
#        workdir=testdir,
#        image="docker://alpine:latest",
#        command='ls $INPUT | tee output.txt',
#        input={
#            'kind': 'directory',
#            'from_path': join(from_path, "testCollection"),
#        })
#
#
#@pytest.fixture
#def workflow_with_file_output():
#    return Run(
#        identifier='workflow_with_file_output',
#        api_url='',
#        workdir=testdir,
#        image="docker://alpine:latest",
#        command='echo "Hello, world!" >> $OUTPUT',
#        output={
#            'kind': 'file',
#            'irods_path': join(from_path, "testCollection"),
#            'from_path': 'output.txt',
#        })
#
#
#@pytest.fixture
#def workflow_with_directory_output():
#    return Run(
#        identifier='workflow_with_directory_output',
#        api_url='',
#        workdir=testdir,
#        image="docker://alpine:latest",
#        command='echo "Hello, world!" | tee $OUTPUT/t1.txt $OUTPUT/t2.txt',
#        output={
#            'kind': 'directory',
#            'irods_path': join(from_path, "testCollection"),
#            'from_path': '',
#        })
#
#
#@pytest.fixture
#def workflow_with_file_input_and_file_output():
#    return Run(
#        identifier='workflow_with_file_input_and_file_output',
#        api_url='',
#        workdir=testdir,
#        image="docker://alpine:latest",
#        command='cat $INPUT | tee $OUTPUT',
#        input={
#            'kind': 'file',
#            'from_path': join(from_path, "testCollection"),
#        },
#        output={
#            'kind': 'file',
#            'irods_path': join(from_path, "testCollection"),
#            'from_path': join(testdir, 'output.txt'),
#        })
#
#
#@pytest.fixture
#def workflow_with_directory_input_and_file_output():
#    return Run(
#        identifier='workflow_with_directory_input_and_file_output',
#        api_url='',
#        workdir=testdir,
#        image="docker://alpine:latest",
#        command='ls $INPUT | tee $OUTPUT',
#        input={
#            'kind': 'directory',
#            'from_path': join(from_path, "testCollection"),
#        },
#        output={
#            'kind': 'file',
#            'irods_path': join(from_path, "testCollection"),
#            'from_path': join(testdir, 'output.txt'),
#        })
#
#
#@pytest.fixture
#def workflow_with_directory_input_and_directory_output():
#    return Run(
#        identifier='workflow_with_directory_input_and_directory_output',
#        api_url='',
#        workdir=testdir,
#        image="docker://alpine:latest",
#        command='cp -r $INPUT $OUTPUT',
#        input={
#            'kind': 'directory',
#            'irods_path': join(from_path, "testCollection"),
#        },
#        output={
#            'kind': 'directory',
#            'irods_path': join(from_path, "testCollection"),
#            'from_path': 'input',
#        })