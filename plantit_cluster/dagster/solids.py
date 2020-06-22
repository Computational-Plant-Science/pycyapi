import subprocess

from dagster import solid

from plantit_cluster.dagster.types import DagsterJob
from plantit_cluster.exceptions import JobException


@solid
def sources(context, job: DagsterJob) -> DagsterJob:
    context.log.info(f"Successfully pulled from sources for job '{job.id}'")
    return job


@solid
def singularity_container(context, job: DagsterJob) -> DagsterJob:
    cmd = [
              "singularity", "exec",
              "--containall",
              "--home", job.workdir,
              job.container
          ] + job.commands
    context.log.info(f"Running Singularity container with '{cmd}' for job '{job.id}'")
    ret = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if ret.returncode != 0:
        msg = f"Non-zero exit code from Singularity container for job '{job.id}': {ret.stderr.decode('utf-8') if ret.stderr else ret.stdout.decode('utf-8') if ret.stdout else 'Unknown error'}"
        context.log.error(msg)
        raise JobException(msg)
    else:
        context.log.info(
            f"Successfully ran Singularity container for job '{job.id}' with output: {ret.stdout.decode('utf-8')}")

    return job


@solid
def docker_container(context, job: DagsterJob) -> DagsterJob:
    cmd = [
              "docker",
              "run",
              job.container
          ] + job.commands
    context.log.info(f"Running Docker container with '{cmd}' for job '{job.id}'")
    ret = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if ret.returncode != 0:
        msg = f"Non-zero exit code from Docker container for job '{job.id}': {ret.stderr.decode('utf-8') if ret.stderr else ret.stdout.decode('utf-8') if ret.stdout else 'Unknown error'}"
        context.log.error(msg)
        raise JobException(msg)
    else:
        context.log.info(
            f"Successfully ran Docker container for job '{job.id}' with output: {ret.stdout.decode('utf-8')}")

    return job


@solid
def targets(context, job: DagsterJob) -> DagsterJob:
    context.log.info(f"Successfully pushed to targets for job '{job.id}'")
    return job
