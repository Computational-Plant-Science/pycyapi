from os.path import join
from tempfile import TemporaryDirectory

from plantit_cli.utils import parse_options

message = "Message"


def test_validate_plan_with_params_and_no_input_and_no_output_and_file_logging():
    with TemporaryDirectory() as test_dir:
        errors, options = parse_options({
            'workdir': test_dir,
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
    with TemporaryDirectory() as test_dir:
        errors, options = parse_options({
            'workdir': test_dir,
            'image': "docker://alpine",
            'command': 'echo "$MESSAGE" >> $MESSAGE_FILE',
            'parameters': [
                {
                    'key': 'MESSAGE',
                    'value': message
                },
                {
                    'key': 'MESSAGE_FILE',
                    'value': join(test_dir, 'message.txt')
                },
            ]
        })
        assert len(errors) == 0

