from os import environ
from uuid import uuid4

import pytest

from plantit.cyverse.auth import CyverseAccessToken
from plantit.scripts import commands as commands
from plantit.scripts.models import ScriptConfig, JobqueueConfig

email = environ.get("TESTING_EMAIL")
queue = environ.get("CLUSTER_QUEUE")
project = environ.get("CLUSTER_PROJECT")
token = CyverseAccessToken.get()


def test_scripts(tmpdir):
    jobqueue = JobqueueConfig(
        queue=queue,
        project=project
    )
    config = ScriptConfig(
        guid=str(uuid4()),
        image="alpine",
        entrypoint="echo 'hello world'",
        workdir=str(tmpdir),
        email=email,
        token=token,
        jobqueue=jobqueue)
    paths = commands.scripts(config)
    assert all(path.is_file() for path in paths)
