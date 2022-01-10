import click
import yaml

from plantit_cli.cli.base import dask
from plantit_cli.runner import dask_commands
from plantit_cli.utils import parse_options


@dask.command()
@click.argument('task')
@click.option('--docker_username', required=False, type=str)
@click.option('--docker_password', required=False, type=str)
@click.option('--docker', is_flag=True)
@click.option('--slurm_job_array', is_flag=True)
def run(task,
        docker_username,
        docker_password,
        docker,
        slurm_job_array):
    with open(task, 'r') as file:
        errors, options = parse_options(yaml.safe_load(file))
        if len(errors) > 0: raise ValueError(f"Invalid configuration: {', '.join(errors)}")
        dask_commands.run_dask(
            options=options,
            docker_username=docker_username,
            docker_password=docker_password,
            docker=docker,
            slurm_job_array=slurm_job_array)