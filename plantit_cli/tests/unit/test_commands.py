import zipfile
from os import environ
from os.path import join, isfile
from tempfile import TemporaryDirectory

import pytest

import plantit_cli.runner.dask_commands
from plantit_cli import commands
from plantit_cli.tests.utils import clear_dir

message = "Message!"


def test_zip_all_files_are_included_by_default(file_name_1, file_name_2):
    zip_stem = 'test_zip'
    zip_name = f"{zip_stem}.zip"

    with TemporaryDirectory() as test_dir:
        try:
            with TemporaryDirectory() as temp_dir:
                with open(join(temp_dir, file_name_1), "w") as file1, \
                        open(join(temp_dir, file_name_2), "w") as file2, \
                        open(join(temp_dir, 'excluded'), "w") as file3:
                    file1.write('Hello, 1!')
                    file2.write('Hello, 2!')
                    file3.write('Hello, 3!')

                commands.zip(input_dir=temp_dir, output_dir=test_dir, name=zip_stem)

            assert isfile(join(test_dir, zip_name))

            with zipfile.ZipFile(join(test_dir, zip_name), 'r') as zip_file:
                zip_file.extractall(test_dir)
                assert isfile(join(test_dir, file_name_1))
                assert isfile(join(test_dir, file_name_2))
        finally:
            clear_dir(test_dir)


# def test_zip_throws_error_when_total_size_exceeds_max(file_name_1, file_name_2):
#     zip_stem = 'test_zip'
#     zip_name = f"{zip_stem}.zip"
#
#     try:
#         with TemporaryDirectory() as temp_dir:
#             with open(join(temp_dir, file_name_1), "w") as file1, \
#                     open(join(temp_dir, file_name_2), "w") as file2, \
#                     open(join(temp_dir, 'excluded'), "w") as file3:
#                 file1.write('Hello, 1!')
#                 file2.write('Hello, 2!')
#                 file3.write('Hello, 3!')
#
#             with pytest.raises(ValueError):
#                 commands.zip(input_dir=temp_dir, output_dir=test_dir, name=zip_stem, max_size=10)
#
#         assert not isfile(join(test_dir, zip_name))
#     finally:
#         clear_dir(test_dir)


def test_clean():
    with TemporaryDirectory() as tempdir:
        path = join(tempdir, 'output.txt')
        with open(path, "w") as file:
            file.write(message)

        pattern = 'sage'
        commands.clean([path], [pattern])

        assert isfile(path)
        with open(path) as file:
            lines = file.readlines()
            assert not any(pattern in line for line in lines)
            assert len(lines) == 1
            assert lines[0] == message.replace(pattern, '****')
