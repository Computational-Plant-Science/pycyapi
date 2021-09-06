import click
import yaml

from plantit_cli import commands
from plantit_cli.store import terrain_commands
from plantit_cli.utils import parse_options


@click.group()
def cli():
    pass


@cli.group()
def terrain():
    pass


@cli.command()
def ping():
    print('pong')


@terrain.command()
@click.argument('remote_path')
@click.option('--local_path', '-p', required=False, type=str)
@click.option('--terrain_token', required=True, type=str)
@click.option('--pattern', required=False, type=str, multiple=True)
@click.option('--overwrite', required=False, type=str, multiple=True)
@click.option('--plantit_token', required=False, type=str)
@click.option('--plantit_url', required=False, type=str)
def pull(
        remote_path,
        terrain_token,
        local_path,
        pattern,
        overwrite,
        plantit_token,
        plantit_url):
    terrain_commands.pull(
        remote_path=remote_path,
        local_path=local_path,
        patterns=pattern,
        overwrite=overwrite,
        cyverse_token=terrain_token,
        plantit_url=plantit_url,
        plantit_token=plantit_token)


@cli.command()
@click.argument('task')
@click.option('--plantit_token', required=False, type=str)
@click.option('--plantit_url', required=False, type=str)
@click.option('--docker_username', required=False, type=str)
@click.option('--docker_password', required=False, type=str)
@click.option('--slurm_job_array', is_flag=True)
def run(task,
        plantit_token,
        plantit_url,
        docker_username,
        docker_password,
        slurm_job_array):
    with open(task, 'r') as file:
        errors, options = parse_options(yaml.safe_load(file))
        if len(errors) > 0: raise ValueError(f"Invalid configuration: {', '.join(errors)}")
        commands.run(
            options=options,
            plantit_url=plantit_url,
            plantit_token=plantit_token,
            docker_username=docker_username,
            docker_password=docker_password,
            slurm_job_array=slurm_job_array)


@click.command()
@click.argument('paths', nargs=-1)
@click.option('--patterns', '-p', multiple=True, type=str)
def clean(paths, patterns):
    commands.clean(paths, patterns)


@cli.command()
@click.argument('input_dir')
@click.option('--output_dir', '-o', required=False, type=str)
@click.option('--name', '-n', required=True, type=str)
# @click.option('--max_size', '-ms', required=False, type=int, default=1000000000)  # 1GB default
@click.option('--include_pattern', '-ip', required=False, type=str, multiple=True)
@click.option('--include_name', '-in', required=False, type=str, multiple=True)
@click.option('--exclude_pattern', '-ep', required=False, type=str, multiple=True)
@click.option('--exclude_name', '-en', required=False, type=str, multiple=True)
@click.option('--plantit_token', required=False, type=str)
@click.option('--plantit_url', required=False, type=str)
def zip(input_dir,
        output_dir,
        name,
        # max_size,
        include_pattern,
        include_name,
        exclude_pattern,
        exclude_name,
        plantit_token,
        plantit_url):
    commands.zip(
        input_dir=input_dir,
        output_dir=output_dir,
        name=name,
        # max_size=max_size,
        include_patterns=include_pattern,
        include_names=include_name,
        exclude_patterns=exclude_pattern,
        exclude_names=exclude_name,
        plantit_url=plantit_url,
        plantit_token=plantit_token)


@terrain.command()
@click.argument('remote_path')
@click.option('--terrain_token', required=True, type=str)
@click.option('--local_path', '-p', required=False, type=str)
@click.option('--include_pattern', '-ip', required=False, type=str, multiple=True)
@click.option('--include_name', '-in', required=False, type=str, multiple=True)
@click.option('--exclude_pattern', '-ep', required=False, type=str, multiple=True)
@click.option('--exclude_name', '-en', required=False, type=str, multiple=True)
@click.option('--plantit_token', required=False, type=str)
@click.option('--plantit_url', required=False, type=str)
def push(remote_path,
         terrain_token,
         local_path,
         include_pattern,
         include_name,
         exclude_pattern,
         exclude_name,
         plantit_token,
         plantit_url):
    terrain_commands.push(
        local_path=local_path,
        remote_path=remote_path,
        cyverse_token=terrain_token,
        include_patterns=include_pattern,
        include_names=include_name,
        exclude_patterns=exclude_pattern,
        exclude_names=exclude_name,
        plantit_url=plantit_url,
        plantit_token=plantit_token)


if __name__ == '__main__':
    cli()
