from os import environ

from click.testing import CliRunner
from pycyapi import cli


def test_cas_token():
    runner = CliRunner()
    username = environ.get('CYVERSE_USERNAME')
    password = environ.get('CYVERSE_PASSWORD')
    result = runner.invoke(cli.cas_token, ['--username', username, '--password', password])
    print(result.output)
    assert result != ''


# TODO: other commands
