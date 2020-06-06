from clusterside.comms import FileComms
from clusterside.executor import Executor
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


def test_local():
    Executor(job).local()


def test_jobqueue_slurm():
    pass
#     Executor(job).jobqueue(
#         queue_type="slurm",
#         cores=1,
#         memory="1MB",
#         processes=1,
#         queue='default',
#         local_directory=job.workdir,
#         walltime="00:01:00")


def test_jobqueue_pbs():
    pass
