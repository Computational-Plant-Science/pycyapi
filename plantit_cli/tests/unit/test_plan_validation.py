from os import environ
from os.path import join

from plantit_cli.config import Config
from plantit_cli.utils import validate_config

message = "Message"
testdir = environ.get('TEST_DIRECTORY')


def test_validate_plan_with_params_and_no_input_and_no_output_and_file_logging():
    plan = Config(
        identifier='test_run_succeeds_with_params_and_no_input_and_no_output',
        workdir=testdir,
        image="docker://alpine",
        command='echo "$MESSAGE" >> $MESSAGE_FILE',
        params=[
            {
                'key': 'MESSAGE',
                'value': message
            },
            {
                'key': 'MESSAGE_FILE',
                'value': 'message.txt'
            },
        ],
        logging={
            'file': 'logfile.txt'
        })
    result = validate_config(plan)
    assert type(result) is bool and result


def test_validate_plan_with_params_and_no_input_and_no_output():
    plan = Config(
        identifier='test_run_succeeds_with_params_and_no_input_and_no_output',
        workdir=testdir,
        image="docker://alpine",
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
    result = validate_config(plan)
    assert type(result) is bool and result

