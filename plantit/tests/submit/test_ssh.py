from os import environ

from plantit.submit.ssh import SSH

CLUSTER_HOST = environ.get('CLUSTER_HOST')
CLUSTER_USER = environ.get('CLUSTER_HOST')
CLUSTER_PASSWORD = environ.get('CLUSTER_PASSWORD')
CLUSTER_KEY_PATH = environ.get('CLUSTER_KEY_PATH')
CLUSTER_HOME_DIR = environ.get('CLUSTER_HOME_DIR')


def test_password_connection():
    with SSH(CLUSTER_HOST, 22, CLUSTER_USER, password=CLUSTER_PASSWORD) as ssh:
        assert ssh.client.get_transport()
        assert ssh.client.get_transport()


def test_key_connection():
    with SSH(CLUSTER_HOST, 22, CLUSTER_USER, pkey=CLUSTER_KEY_PATH) as ssh:
        assert ssh.client.get_transport()
        assert ssh.client.get_transport().is_active()


def test_command():
    with SSH(CLUSTER_HOST, 22, CLUSTER_USER, pkey=CLUSTER_KEY_PATH) as ssh:
        stdin, stdout, stderr = ssh.client.exec_command('pwd')
        assert f"/{CLUSTER_HOME_DIR}\n" == stdout.readlines()[0]
