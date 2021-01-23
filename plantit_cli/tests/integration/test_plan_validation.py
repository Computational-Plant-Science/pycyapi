from os import environ
from os.path import join

import pytest

from plantit_cli.options import RunOptions
from plantit_cli.tests.integration.terrain_test_utils import create_collection, delete_collection, upload_file
from plantit_cli.tests.test_utils import get_token, clear_dir
from plantit_cli.utils import parse_options

message = "Message"
testdir = environ.get('TEST_DIRECTORY')
token = get_token()


@pytest.fixture
def remote_path(remote_base_path):
    path = join(remote_base_path, "testCollection")
    return path


def test_validate_plan_with_no_params_and_file_input_and_no_output(remote_path, file_name_1):
    local_path = join(testdir, file_name_1)
    plan = RunOptions(
        identifier='test_run_succeeds_with_no_params_and_file_input_and_no_output',
        workdir=testdir,
        image="docker://alpine",
        command='cat "$INPUT" | tee "$INPUT.output"',
        input={
            'kind': 'file',
            'from': join(remote_path, file_name_1),
        },
        cyverse_token=token)

    try:
        # prep CyVerse collection
        create_collection(remote_path, token)

        # prep file
        with open(local_path, "w") as file1:
            file1.write('Hello, 1!')
        upload_file(local_path, remote_path, token)

        result = parse_options(plan)
        assert type(result) is bool and result
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)


def test_validate_plan_with_params_and_file_input_and_no_output(remote_path, file_name_1):
    local_path = join(testdir, file_name_1)
    plan = RunOptions(
        identifier='test_run_succeeds_with_params_and_file_input_and_no_output',
        workdir=testdir,
        image="docker://alpine",
        command='cat "$INPUT" | tee "$INPUT.$TAG.output"',
        input={
            'kind': 'file',
            'from': join(remote_path, file_name_1),
        },
        parameters=[
            {
                'key': 'TAG',
                'value': message
            },
        ],
        cyverse_token=token)

    try:
        # prep CyVerse collection
        create_collection(remote_path, token)

        # prep file
        with open(local_path, "w") as file1:
            file1.write('Hello, 1!')
        upload_file(local_path, remote_path, token)

        result = parse_options(plan)
        assert type(result) is bool and result
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)


def test_validate_plan_with_no_params_and_file_input_and_no_output_when_no_inputs_found(remote_path, file_name_1):
    local_path = join(testdir, file_name_1)
    plan = RunOptions(
        identifier='test_run_fails_with_no_params_and_file_input_and_no_output_when_no_inputs_found',
        workdir=testdir,
        image="docker://alpine",
        command='cat "$INPUT" | tee "$INPUT.output"',
        input={
            'kind': 'file',
            'from': join(remote_path, file_name_1),
        },
        cyverse_token=token)

    try:
        # prep CyVerse collection
        create_collection(remote_path, token)

        # prep file
        with open(local_path, "w") as file1:
            file1.write('Hello, 1!')
        upload_file(local_path, remote_path, token)

        result = parse_options(plan)
        assert type(result) is bool and result
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)


def test_validate_plan_with_params_and_file_input_and_no_output_when_no_inputs_found(remote_path, file_name_1):
    local_path = join(testdir, file_name_1)
    plan = RunOptions(
        identifier='test_run_fails_with_params_and_file_input_and_no_output_when_no_inputs_found',
        workdir=testdir,
        image="docker://alpine",
        command='cat "$INPUT" | tee "$INPUT.$TAG.output"',
        input={
            'kind': 'file',
            'from': join(remote_path, file_name_1),
        },
        parameters=[
            {
                'key': 'TAG',
                'value': message
            },
        ],
        cyverse_token=token)

    try:
        # prep CyVerse collection
        create_collection(remote_path, token)

        # prep file
        with open(local_path, "w") as file1:
            file1.write('Hello, 1!')
        upload_file(local_path, remote_path, token)

        result = parse_options(plan)
        assert type(result) is bool and result
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)


def test_validate_plan_with_no_params_and_files_input_and_no_output(remote_path, file_name_1, file_name_2):
    local_path_1 = join(testdir, file_name_1)
    local_path_2 = join(testdir, file_name_2)
    plan = RunOptions(
        identifier='test_run_succeeds_with_no_params_and_files_input_and_no_output',
        workdir=testdir,
        image="docker://alpine",
        command='cat $INPUT | tee $INPUT.output',
        input={
            'kind': 'files',
            'from': remote_path,
        },
        cyverse_token=token)

    try:
        # prep CyVerse collection
        create_collection(remote_path, token)

        # prep files
        with open(local_path_1, "w") as file1, open(local_path_2, "w") as file2:
            file1.write('Hello, 1!')
            file2.write('Hello, 2!')
        upload_file(local_path_1, remote_path, token)
        upload_file(local_path_2, remote_path, token)

        result = parse_options(plan)
        assert type(result) is bool and result
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)


def test_validate_plan_with_params_and_files_input_and_no_output(remote_path, file_name_1, file_name_2):
    local_path_1 = join(testdir, file_name_1)
    local_path_2 = join(testdir, file_name_2)
    plan = RunOptions(
        identifier='test_run_succeeds_with_params_and_files_input_and_no_output',
        workdir=testdir,
        image="docker://alpine",
        command='cat $INPUT | tee $INPUT.$TAG.output',
        input={
            'kind': 'files',
            'from': remote_path,
        },
        parameters=[
            {
                'key': 'TAG',
                'value': message
            },
        ],
        cyverse_token=token)

    try:
        # prep CyVerse collection
        create_collection(remote_path, token)

        # prep files
        with open(local_path_1, "w") as file1, open(local_path_2, "w") as file2:
            file1.write('Hello, 1!')
            file2.write('Hello, 2!')
        upload_file(local_path_1, remote_path, token)
        upload_file(local_path_2, remote_path, token)

        result = parse_options(plan)
        assert type(result) is bool and result
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)


def test_validate_plan_with_params_and_no_input_and_file_output(remote_path, file_name_1):
    plan = RunOptions(
        identifier='test_run_succeeds_with_params_and_no_input_and_file_output',
        workdir=testdir,
        image="docker://alpine",
        command='echo "Hello, world!" >> $OUTPUT',
        output={
            'to': remote_path,
            'from': f"output.{message}.txt",
        },
        parameters=[
            {
                'key': 'TAG',
                'value': message
            },
        ],
        cyverse_token=token)

    try:
        # prep CyVerse collection
        create_collection(remote_path, token)

        result = parse_options(plan)
        assert type(result) is bool and result
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)


def test_validate_plan_with_no_params_and_no_input_and_directory_output(remote_path, file_name_1):
    plan = RunOptions(
        identifier='test_run_succeeds_with_no_params_and_no_input_and_directory_output',
        workdir=testdir,
        image="docker://alpine",
        command='echo "Hello, world!" | tee $OUTPUT/t1.txt $OUTPUT/t2.txt',
        output={
            'to': remote_path,
            'from': '',
        },
        cyverse_token=token)

    try:
        # prep CyVerse collection
        create_collection(remote_path, token)

        result = parse_options(plan)
        assert type(result) is bool and result
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)


def test_validate_plan_with_params_and_no_input_and_directory_output(remote_path, file_name_1):
    plan = RunOptions(
        identifier='test_run_succeeds_with_params_and_no_input_and_directory_output',
        workdir=testdir,
        image="docker://alpine",
        command='echo "Hello, world!" | tee $OUTPUT/t1.$TAG.txt $OUTPUT/t2.$TAG.txt',
        output={
            'to': remote_path,
            'from': '',
        },
        parameters=[
            {
                'key': 'TAG',
                'value': message
            },
        ],
        cyverse_token=token)

    try:
        # prep CyVerse collection
        create_collection(remote_path, token)

        result = parse_options(plan)
        assert type(result) is bool and result
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)


def test_validate_plan_with_no_params_and_file_input_and_directory_output(remote_path, file_name_1):
    local_path = join(testdir, file_name_1)
    plan = RunOptions(
        identifier='test_run_succeeds_with_no_params_and_file_input_and_directory_output',
        workdir=testdir,
        image="docker://alpine",
        command='cat $INPUT | tee $INPUT.output',
        input={
            'kind': 'file',
            'from': join(remote_path, file_name_1),
        },
        output={
            'to': remote_path,
            'from': 'input',  # write output files to input dir
            'include': {
                'patterns': ['output'],
                'names': []}
        },
        cyverse_token=token)

    try:
        # prep CyVerse collection
        create_collection(remote_path, token)

        # prep file
        with open(local_path, "w") as file1:
            file1.write('Hello, 1!')
        upload_file(local_path, remote_path, token)

        result = parse_options(plan)
        assert type(result) is bool and result
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)


def test_validate_plan_with_params_and_file_input_and_directory_output(remote_path, file_name_1):
    local_path = join(testdir, file_name_1)
    plan = RunOptions(
        identifier='test_run_succeeds_with_params_and_file_input_and_directory_output',
        workdir=testdir,
        image="docker://alpine",
        command='cat $INPUT | tee $INPUT.$TAG.output',
        input={
            'kind': 'file',
            'from': join(remote_path, file_name_1),
        },
        output={
            'to': remote_path,
            'from': 'input',  # write output files to input dir
            'include': {
                'patterns': ['output'],
                'names': []
            }
        },
        parameters=[
            {
                'key': 'TAG',
                'value': message
            },
        ],
        cyverse_token=token)

    try:
        # prep CyVerse collection
        create_collection(remote_path, token)

        # prep file
        with open(local_path, "w") as file1:
            file1.write('Hello, 1!')
        upload_file(local_path, remote_path, token)

        result = parse_options(plan)
        assert type(result) is bool and result
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)


def test_validate_plan_with_no_params_and_files_input_and_directory_output(remote_path,
                                                                           file_name_1,
                                                                           file_name_2):
    local_input_file_path_1 = join(testdir, file_name_1)
    local_input_file_path_2 = join(testdir, file_name_2)
    plan = RunOptions(
        identifier='test_run_succeeds_with_no_params_and_files_input_and_directory_output',
        workdir=testdir,
        image="docker://alpine",
        command='cat $INPUT | tee $INPUT.output',
        input={
            'kind': 'files',
            'from': remote_path,
        },
        output={
            'to': remote_path,
            'from': 'input',  # write output files to input dir
            'include': {
                'patterns': ['output'],
                'names': []
            }
        },
        cyverse_token=token)

    try:
        # prep CyVerse collection
        create_collection(remote_path, token)

        # prep file
        with open(local_input_file_path_1, "w") as file1, open(local_input_file_path_2, "w") as file2:
            file1.write('Hello, 1!')
            file2.write('Hello, 2!')
        upload_file(local_input_file_path_1, remote_path, token)
        upload_file(local_input_file_path_2, remote_path, token)

        result = parse_options(plan)
        assert type(result) is bool and result
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)


def test_validate_plan_with_params_and_files_input_and_directory_output(remote_path,
                                                                        file_name_1,
                                                                        file_name_2):
    local_input_file_path_1 = join(testdir, file_name_1)
    local_input_file_path_2 = join(testdir, file_name_2)
    plan = RunOptions(
        identifier='test_run_succeeds_with_params_and_files_input_and_directory_output',
        workdir=testdir,
        image="docker://alpine",
        command='cat $INPUT | tee $INPUT.$TAG.output',
        input={
            'kind': 'files',
            'from': remote_path,
        },
        output={
            'to': remote_path,
            'from': 'input',  # write output files to input dir
            'include': {
                'patterns': ['output'],
                'names': []
            }
        },
        parameters=[
            {
                'key': 'TAG',
                'value': message
            },
        ],
        cyverse_token=token)

    try:
        # prep CyVerse collection
        create_collection(remote_path, token)

        # prep file
        with open(local_input_file_path_1, "w") as file1, open(local_input_file_path_2, "w") as file2:
            file1.write('Hello, 1!')
            file2.write('Hello, 2!')
        upload_file(local_input_file_path_1, remote_path, token)
        upload_file(local_input_file_path_2, remote_path, token)

        result = parse_options(plan)
        assert type(result) is bool and result
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)


def test_validate_plan_with_params_and_files_input_with_pattern_and_directory_output(remote_path,
                                                                                     file_name_1,
                                                                                     file_name_2):
    local_input_file_path_1 = join(testdir, file_name_1)
    local_input_file_path_2 = join(testdir, file_name_2)
    plan = RunOptions(
        identifier='test_validate_plan_with_params_and_files_input_with_pattern_and_directory_output',
        workdir=testdir,
        image="docker://alpine",
        command='cat $INPUT | tee $INPUT.$TAG.output',
        input={
            'kind': 'files',
            'from': remote_path,
            'patterns': [
                file_name_1
            ]
        },
        output={
            'to': remote_path,
            'from': 'input',  # write output files to input dir
            'include': {
                'patterns': ['output'],
                'names': []
            }
        },
        parameters=[
            {
                'key': 'TAG',
                'value': message
            },
        ],
        cyverse_token=token)

    try:
        # prep CyVerse collection
        create_collection(remote_path, token)

        # prep file
        with open(local_input_file_path_1, "w") as file1, open(local_input_file_path_2, "w") as file2:
            file1.write('Hello, 1!')
            file2.write('Hello, 2!')
        upload_file(local_input_file_path_1, remote_path, token)
        upload_file(local_input_file_path_2, remote_path, token)

        result = parse_options(plan)
        assert type(result) is bool and result
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)


def test_validate_plan_with_no_params_and_directory_input_and_directory_output(remote_path,
                                                                               file_name_1,
                                                                               file_name_2):
    local_input_file_path_1 = join(testdir, file_name_1)
    local_input_file_path_2 = join(testdir, file_name_2)
    plan = RunOptions(
        identifier='test_run_succeeds_with_no_params_and_directory_input_and_directory_output',
        workdir=testdir,
        image="docker://alpine",
        command='ls $INPUT | tee $INPUT.output',
        input={
            'kind': 'directory',
            'from': remote_path,
        },
        output={
            'to': remote_path,
            'from': '',
            'include': {
                'patterns': ['output'],
                'names': []
            }
        },
        cyverse_token=token)

    try:
        # prep CyVerse collection
        create_collection(remote_path, token)

        # prep file
        with open(local_input_file_path_1, "w") as file1, open(local_input_file_path_2, "w") as file2:
            file1.write('Hello, 1!')
            file2.write('Hello, 2!')
        upload_file(local_input_file_path_1, remote_path, token)
        upload_file(local_input_file_path_2, remote_path, token)

        result = parse_options(plan)
        assert type(result) is bool and result
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)


def test_validate_plan_with_params_and_directory_input_and_directory_output(remote_path,
                                                                            file_name_1,
                                                                            file_name_2):
    local_input_file_path_1 = join(testdir, file_name_1)
    local_input_file_path_2 = join(testdir, file_name_2)
    plan = RunOptions(
        identifier='test_run_succeeds_with_params_and_directory_input_and_directory_output',
        workdir=testdir,
        image="docker://alpine",
        command='ls $INPUT | tee $INPUT.$TAG.output',
        input={
            'kind': 'directory',
            'from': remote_path,
        },
        output={
            'to': remote_path,
            'from': '',
            'include': {
                'patterns': ['output'],
                'names': []
            }
        },
        parameters=[
            {
                'key': 'TAG',
                'value': message
            },
        ],
        cyverse_token=token)

    try:
        # prep CyVerse collection
        create_collection(remote_path, token)

        # prep file
        with open(local_input_file_path_1, "w") as file1, open(local_input_file_path_2, "w") as file2:
            file1.write('Hello, 1!')
            file2.write('Hello, 2!')
        upload_file(local_input_file_path_1, remote_path, token)
        upload_file(local_input_file_path_2, remote_path, token)

        result = parse_options(plan)
        assert type(result) is bool and result
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)


def test_validate_plan_with_no_params_and_directory_input_and_directory_output_when_no_inputs_found(remote_path,
                                                                                                    file_name_1,
                                                                                                    file_name_2):
    local_input_file_path_1 = join(testdir, file_name_1)
    local_input_file_path_2 = join(testdir, file_name_2)
    plan = RunOptions(
        identifier='test_run_fails_with_no_params_and_directory_input_and_directory_output_when_no_inputs_found',
        workdir=testdir,
        image="docker://alpine",
        command='ls $INPUT | tee $INPUT.output',
        input={
            'kind': 'directory',
            'from': remote_path,
        },
        output={
            'to': remote_path,
            'from': '',
            'include': {
                'patterns': ['output'],
                'names': []
            }
        },
        cyverse_token=token)

    try:
        # prep CyVerse collection
        create_collection(remote_path, token)

        # prep file
        with open(local_input_file_path_1, "w") as file1, open(local_input_file_path_2, "w") as file2:
            file1.write('Hello, 1!')
            file2.write('Hello, 2!')
        upload_file(local_input_file_path_1, remote_path, token)
        upload_file(local_input_file_path_2, remote_path, token)

        result = parse_options(plan)
        assert type(result) is bool and result
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)


def test_validate_plan_with_params_and_directory_input_and_directory_output_when_no_inputs_found(
        remote_path,
        file_name_1,
        file_name_2):
    local_input_file_path_1 = join(testdir, file_name_1)
    local_input_file_path_2 = join(testdir, file_name_2)
    plan = RunOptions(
        identifier='test_run_fails_with_params_and_directory_input_and_directory_output_when_no_inputs_found',
        workdir=testdir,
        image="docker://alpine",
        command='ls $INPUT | tee $INPUT.$TAG.output',
        input={
            'kind': 'directory',
            'from': remote_path,
        },
        output={
            'to': remote_path,
            'from': '',
            'include': {
                'patterns': ['output'],
                'names': []
            }
        },
        parameters=[
            {
                'key': 'TAG',
                'value': message
            },
        ],
        cyverse_token=token)

    try:
        # prep CyVerse collection
        create_collection(remote_path, token)

        # prep file
        with open(local_input_file_path_1, "w") as file1, open(local_input_file_path_2, "w") as file2:
            file1.write('Hello, 1!')
            file2.write('Hello, 2!')
        upload_file(local_input_file_path_1, remote_path, token)
        upload_file(local_input_file_path_2, remote_path, token)

        result = parse_options(plan)
        assert type(result) is bool and result
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)


def test_validate_plan_with_no_params_and_no_input_and_directory_output_with_include_patterns_and_exclude_names(
        remote_path):
    plan = RunOptions(
        identifier='test_run_succeeds_with_no_params_and_no_input_and_directory_output_with_excludes',
        workdir=testdir,
        image="docker://alpine",
        command='touch excluded.output included.output',
        output={
            'to': remote_path,
            'from': '',
            'include': {
                'patterns': ['output'],
                'names': []
            },
            'exclude': {
                'patterns': [],
                'names': ['excluded.output']
            }
        },
        cyverse_token=token)

    try:
        # prep CyVerse collection
        create_collection(remote_path, token)

        result = parse_options(plan)
        assert type(result) is bool and result
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)


def test_validate_plan_with_params_and_no_input_and_directory_output_with_excludes(remote_path):
    plan = RunOptions(
        identifier='test_run_succeeds_with_params_and_no_input_and_directory_output_with_excludes',
        workdir=testdir,
        image="docker://alpine",
        command='touch excluded.output included.$TAG.output',
        output={
            'to': remote_path,
            'from': '',
            'include': {
                'patterns': ['output'],
                'names': []
            },
            'exclude': {
                'patterns': [],
                'names': ['excluded.output']
            }
        },
        parameters=[
            {
                'key': 'TAG',
                'value': message
            },
        ],
        cyverse_token=token)

    try:
        # prep CyVerse collection
        create_collection(remote_path, token)

        result = parse_options(plan)
        assert type(result) is bool and result
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)


def test_validate_plan_with_no_params_and_no_input_and_directory_output_with_non_matching_case_pattern_and_excludes(
        remote_path):
    plan = RunOptions(
        identifier='test_run_succeeds_with_no_params_and_no_input_and_directory_output_with_non_matching_case_pattern_and_excludes',
        workdir=testdir,
        image="docker://alpine",
        command='touch excluded.output included.output',
        output={
            'to': remote_path,
            'from': '',
            'include': {
                'patterns': ['OUTPUT'],
                'names': []
            },
            'exclude': {
                'patterns': [],
                'names': ['excluded.output']
            }
        },
        cyverse_token=token)

    try:
        # prep CyVerse collection
        create_collection(remote_path, token)

        result = parse_options(plan)
        assert type(result) is bool and result
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)


def test_validate_plan_with_params_and_no_input_and_directory_output_with_non_matching_case_pattern_and_excludes(
        remote_path):
    plan = RunOptions(
        identifier='test_run_succeeds_with_params_and_no_input_and_directory_output_with_non_matching_case_pattern_and_excludes',
        workdir=testdir,
        image="docker://alpine",
        command='touch excluded.output included.$TAG.output',
        output={
            'to': remote_path,
            'from': '',
            'include': {
                'patterns': ['OUTPUT'],
                'names': []
            },
            'exclude': {
                'patterns': [],
                'names': ['excluded.output']
            }
        }
        ,
        parameters=[
            {
                'key': 'TAG',
                'value': message
            },
        ],
        cyverse_token=token)

    try:
        # prep CyVerse collection
        create_collection(remote_path, token)

        result = parse_options(plan)
        assert type(result) is bool and result
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)
