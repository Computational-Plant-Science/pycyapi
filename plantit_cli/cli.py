import click
import yaml

from plantit_cli.executor.inprocessexecutor import InProcessExecutor
from plantit_cli.executor.jobqueueexecutor import JobQueueExecutor
from plantit_cli.run import Run
from plantit_cli.store.irods import IRODSOptions


@click.command()
@click.argument('workflow')
@click.option('--token', required=False, type=str)
@click.option('--irods_host', required=False, type=str)
@click.option('--irods_port', required=False, type=int)
@click.option('--irods_username', required=False, type=str)
@click.option('--irods_password', required=False, type=str)
@click.option('--irods_zone', required=False, type=str)
def run(workflow, token, irods_host, irods_port, irods_username, irods_password, irods_zone):
    with open(workflow, 'r') as file:
        definition = yaml.safe_load(file)
        definition['token'] = token
        executor = definition['executor']
        del definition['executor']
        irods_options = None if irods_host is None else IRODSOptions(irods_host,
                                                                     irods_port,
                                                                     irods_username,
                                                                     irods_password,
                                                                     irods_zone)

        if 'in-process' in executor:
            InProcessExecutor(irods_options).execute(Run(**definition))
        elif 'pbs' in executor:
            JobQueueExecutor(irods_options, **executor['pbs']).execute(Run(**definition))
        elif 'slurm' in executor['name']:
            JobQueueExecutor(irods_options, **executor['slurm']).execute(Run(**definition))
        else:
            raise ValueError(f"Unrecognized executor (supported: 'in-process', 'pbs', 'slurm')")
