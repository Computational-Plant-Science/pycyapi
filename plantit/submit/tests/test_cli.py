import pytest
from click.testing import CliRunner

from plantit.submit import cli


@pytest.mark.skip(reason='todo')
@pytest.mark.slow
def test_submit_slurm():
    runner = CliRunner()
    # result = runner.invoke(cli.submit, [""])
    # todo
