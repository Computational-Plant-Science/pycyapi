from os import environ
from pathlib import Path

from plantit.submit import commands as commands
from plantit.submit.models import SubmitConfig
from plantit.submit.ssh import SSH

CLUSTER_HOST = environ.get("CLUSTER_HOST")
CLUSTER_USER = environ.get("CLUSTER_USER")
CLUSTER_PASSWORD = environ.get("CLUSTER_PASSWORD")
CLUSTER_KEY_PATH = environ.get("CLUSTER_KEY_PATH")
CLUSTER_HOME_DIR = environ.get("CLUSTER_HOME_DIR")
SLURM_JOB_SCRIPT = environ.get("SLURM_JOB_SCRIPT")
LOCAL_SCRIPT_PATH = Path(__file__).parent / "slurm_template.sh"
REMOTE_SCRIPT_PATH = Path(CLUSTER_HOME_DIR) / LOCAL_SCRIPT_PATH.name


def upload_script(local, remote):
    with SSH(
        host=CLUSTER_HOST, port=22, username=CLUSTER_USER, password=CLUSTER_PASSWORD
    ) as client:
        with client.open_sftp() as sftp:
            sftp.put(str(local), str(remote))


def test_submit_with_password_auth():
    upload_script(LOCAL_SCRIPT_PATH, REMOTE_SCRIPT_PATH)

    config = SubmitConfig(
        host=CLUSTER_HOST,
        port=22,
        username=CLUSTER_USER,
        password=CLUSTER_PASSWORD,
        workdir=CLUSTER_HOME_DIR,
        script=str(REMOTE_SCRIPT_PATH),
    )
    job_id = commands.submit(config)
    assert job_id.isdigit()


def test_submit_with_key_auth():
    upload_script(LOCAL_SCRIPT_PATH, REMOTE_SCRIPT_PATH)

    config = SubmitConfig(
        host=CLUSTER_HOST,
        port=22,
        username=CLUSTER_USER,
        key=CLUSTER_KEY_PATH,
        workdir=CLUSTER_HOME_DIR,
        script=str(REMOTE_SCRIPT_PATH),
    )
    job_id = commands.submit(config)
    assert job_id.isdigit()
