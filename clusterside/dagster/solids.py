import subprocess
import traceback

from dagster import solid

from clusterside.comms import RESTComms
from clusterside.workflow import Workflow


@solid
def extract_inputs(context, workflow: Workflow) -> Workflow:
    return workflow


@solid
def run_pre_commands(context, workflow: Workflow) -> Workflow:
    server = RESTComms(url=workflow.server_url,
                       headers={"Authorization": "Token " + workflow.token})

    try:
        if workflow.pre_commands:
            ret = subprocess.run(workflow.pre_commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if ret.returncode != 0:
                msg = f"Non-zero exit code while running job '{workflow.job_pk}' pre-commands: {ret.stderr.decode('utf-8') if ret.stderr else ret.stdout.decode('utf-8') if ret.stdout else 'Unknown error'}"
                context.log.error(msg)
                server.update_status(workflow.job_pk, server.FAILED, msg)
            else:
                msg = f"Successfully ran job '{workflow.job_pk}' pre-commands"
                context.log.info(msg)
                server.update_status(workflow.job_pk, server.OK, msg)
        else:
            msg = f"No pre-commands configured for job '{workflow.job_pk}', skipping"
            context.log.info(msg)
            server.update_status(workflow.job_pk, server.OK, msg)

    except Exception:
        msg = f"Failed to run job '{workflow.job_pk}' pre-commands: {traceback.format_exc()}"
        context.log.error(msg)
        server.update_status(workflow.job_pk, server.FAILED, msg)

    return workflow


@solid
def run_container(context, workflow: Workflow) -> Workflow:
    server = RESTComms(url=workflow.server_url,
                       headers={"Authorization": "Token " + workflow.token})

    try:
        cmd = [
                  "singularity",
                  "exec"
              ] + [f for f in workflow.flags] + [
                  "--containall",
                  workflow.container_url,
                  workflow.commands
              ]
        ret = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if ret.returncode != 0:
            msg = f"Non-zero exit code while running job '{workflow.job_pk}': {ret.stderr.decode('utf-8') if ret.stderr else ret.stdout.decode('utf-8') if ret.stdout else 'Unknown error'}"
            context.log.error(msg)
            server.update_status(workflow.job_pk, server.FAILED, msg)
        else:
            msg = f"Successfully ran job '{workflow.job_pk}'"
            context.log.info(msg)
            server.update_status(workflow.job_pk, server.OK, msg)

    except Exception:
        msg = f"Failed to run job '{workflow.job_pk}': {traceback.format_exc()}"
        context.log.error(msg)
        server.update_status(workflow.job_pk, server.FAILED, msg)

    return workflow


@solid
def run_post_commands(context, workflow: Workflow) -> Workflow:
    server = RESTComms(url=workflow.server_url,
                       headers={"Authorization": "Token " + workflow.token})

    try:
        if workflow.post_commands:
            ret = subprocess.run(workflow.post_commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if ret.returncode != 0:
                msg = f"Non-zero exit code while running job '{workflow.job_pk}' post-commands: {ret.stderr.decode('utf-8') if ret.stderr else ret.stdout.decode('utf-8') if ret.stdout else 'Unknown error'}"
                context.log.error(msg)
                server.update_status(workflow.job_pk, server.FAILED, msg)
            else:
                msg = f"Successfully ran job '{workflow.job_pk}' post-commands"
                context.log.info(msg)
                server.update_status(workflow.job_pk, server.OK, msg)
        else:
            msg = f"No post-commands configured for job '{workflow.job_pk}', skipping"
            context.log.info(msg)
            server.update_status(workflow.job_pk, server.OK, msg)

    except Exception:
        msg = f"Failed to run job '{workflow.job_pk}' post-commands: {traceback.format_exc()}"
        context.log.error(msg)
        server.update_status(workflow.job_pk, server.FAILED, msg)

    return workflow


@solid
def load_outputs(context, workflow: Workflow) -> Workflow:
    return workflow
