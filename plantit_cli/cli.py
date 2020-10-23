import json

import click
import yaml

from plantit_cli.executor.local import LocalExecutor
from plantit_cli.executor.jobqueue import JobQueueExecutor
from plantit_cli.run import Run


@click.command()
@click.argument('workflow')
@click.option('--plantit_token', required=False, type=str)
@click.option('--cyverse_token', required=False, type=str)
def run(workflow, plantit_token, cyverse_token):
    with open(workflow, 'r') as file:
        workflow_def = yaml.safe_load(file)
        workflow_def['plantit_token'] = plantit_token
        workflow_def['cyverse_token'] = cyverse_token

        if 'api_url' not in workflow_def:
            workflow_def['api_url'] = None

        if 'executor' in workflow_def:
            executor_def = workflow_def['executor']
            del workflow_def['executor']
        else:
            executor_def = {'local'}

        if 'local' in executor_def:
            LocalExecutor(cyverse_token).execute(Run(**workflow_def))
        elif 'jobqueue' in executor_def and 'slurm' in executor_def['jobqueue']:
            executor_def = dict(executor_def['jobqueue']['slurm'])
            JobQueueExecutor('slurm', cyverse_token, **executor_def).execute(Run(**workflow_def))
        else:
            raise ValueError(f"Unrecognized executor (supported: 'local', 'jobqueue')")
