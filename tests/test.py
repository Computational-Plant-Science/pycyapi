from clusterside import clusterside
from clusterside.job import Job


definition = {
    "id": "2",
    "workdir": '/',
    "token": "token",
    "server": "",
    "container": "docker://alpine:latest",
    "commands": "/bin/ash -c 'pwd'"
}

job = Job(
    id=definition['id'],
    workdir=definition['workdir'],
    token=definition['token'],
    server=definition['server'],
    container=definition['container'],
    commands=definition['commands'].split())

clusterside.Clusterside(job).in_process()
