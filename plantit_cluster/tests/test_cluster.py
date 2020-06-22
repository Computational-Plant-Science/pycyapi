from plantit_cluster.cluster import Cluster
from plantit_cluster.job import Job

definition = {
    "id": "2",
    "workdir": '/',
    "token": "token",
    "server": "",
    "container": "docker://alpine:latest",
    "commands": "/bin/ash -c 'pwd'",
    "executor": {
        "type": "local"
    }
}

job = Job(
    id=definition['id'],
    workdir=definition['workdir'],
    token=definition['token'],
    server=definition['server'],
    container=definition['container'],
    commands=definition['commands'].split(),
    executor=definition['executor'])


def test_local():
    Cluster(job).run()

