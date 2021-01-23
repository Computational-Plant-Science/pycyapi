from os import environ
from os.path import join

from plantit_cli.utils import parse_options

message = "Message"
testdir = environ.get('TEST_DIRECTORY')


def test_validate_plan_with_params_and_no_input_and_no_output_and_file_logging():
    errors, options = parse_options({
        'workdir': testdir,
        'image': "docker://alpine",
        'command': 'echo "$MESSAGE" >> $MESSAGE_FILE',
        'parameters': [
            {
                'key': 'MESSAGE',
                'value': message
            },
            {
                'key': 'MESSAGE_FILE',
                'value': 'message.txt'
            },
        ],
        'log_file': 'logfile.txt'
    })
    assert len(errors) == 0


def test_validate_plan_with_params_and_no_input_and_no_output():
    errors, options = parse_options({
        'workdir': testdir,
        'image': "docker://alpine",
        'command': 'echo "$MESSAGE" >> $MESSAGE_FILE',
        'parameters': [
            {
                'key': 'MESSAGE',
                'value': message
            },
            {
                'key': 'MESSAGE_FILE',
                'value': join(testdir, 'message.txt')
            },
        ]
    })
    assert len(errors) == 0

