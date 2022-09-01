from os import environ

from plantit.submit.ssh import SSH

CLUSTER_HOST = environ.get("CLUSTER_HOST")
CLUSTER_USER = environ.get("CLUSTER_USER")
CLUSTER_PASSWORD = environ.get("CLUSTER_PASSWORD")
CLUSTER_HOME_DIR = environ.get("CLUSTER_HOME_DIR")


def test_password_auth():
    with SSH(CLUSTER_HOST, 22, CLUSTER_USER, password=CLUSTER_PASSWORD) as client:
        assert client.get_transport()
        assert client.get_transport().is_active()


def test_key_auth():
    with SSH(CLUSTER_HOST, 22, CLUSTER_USER) as client:
        assert client.get_transport()
        assert client.get_transport().is_active()


def test_command():
    with SSH(CLUSTER_HOST, 22, CLUSTER_USER) as client:
        stdin, stdout, stderr = client.exec_command("pwd")
        assert f"{CLUSTER_HOME_DIR}\n" == stdout.readlines()[0]
