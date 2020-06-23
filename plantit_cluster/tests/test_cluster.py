from plantit_cluster.cluster import Cluster
from plantit_cluster.job import Job

job = Job(
    id="2",
    workdir="/test",
    token="token",
    server="",
    container="docker://alpine:latest",
    commands=["/bin/ash", "-c", "'pwd'"],
    executor={"name": "local"}
)


def test_run_inprocess():
    Cluster.run_inprocess(job)

