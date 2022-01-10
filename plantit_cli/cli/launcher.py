import click
import yaml

from plantit_cli.cli.base import launcher
from plantit_cli.runner import launcher_commands
from plantit_cli.utils import parse_options


@launcher.command()
@click.argument('task')
@click.option('--docker_username', required=False, type=str)
@click.option('--docker_password', required=False, type=str)
@click.option('--docker', is_flag=True)
@click.option('--slurm_job_array', is_flag=True)
def run(task,
        docker_username,
        docker_password,
        docker):
    with open(task, 'r') as file:
        errors, options = parse_options(yaml.safe_load(file))
        if len(errors) > 0: raise ValueError(f"Invalid configuration: {', '.join(errors)}")
        launcher_commands.run_launcher(
            options=options,
            docker_username=docker_username,
            docker_password=docker_password,
            docker=docker)