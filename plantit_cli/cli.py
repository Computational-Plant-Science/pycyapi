import click
import yaml

from plantit_cli.executor.inprocessexecutor import InProcessExecutor
from plantit_cli.executor.jobqueueexecutor import JobQueueExecutor
from plantit_cli.run import Run


@click.command()
@click.argument('workflow')
@click.option('--token')
def run(workflow, token):
    with open(workflow, 'r') as file:
        definition = yaml.safe_load(file)
        definition['token'] = token
        executor = definition['executor']
        del definition['executor']

        if 'in-process' in executor:
            InProcessExecutor().execute(Run(**definition))
        elif 'pbs' in executor:
            JobQueueExecutor(**executor['pbs']).execute(Run(**definition))
        elif 'slurm' in executor['name']:
            JobQueueExecutor(**executor['slurm']).execute(Run(**definition))
        else:
            raise ValueError(f"Unrecognized executor (supported: 'in-process', 'pbs', 'slurm')")

