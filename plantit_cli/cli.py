import click
import yaml

from plantit_cli import commands
from plantit_cli.utils import parse_options


@click.group()
def cli():
    pass


@click.command()
@click.argument('input_dir')
@click.option('-o', '--output_dir', required=False, type=str)
@click.option('-n', '--name', required=False, type=str)
@click.option('-ms', '--max_size', required=False, type=int, default=1000000000)  # 1GB default
@click.option('-ip', '--include_patterns', required=False, type=str, multiple=True)
@click.option('-in', '--include_names', required=False, type=str, multiple=True)
@click.option('-ep', '--exclude_patterns', required=False, type=str, multiple=True)
@click.option('-en', '--exclude_names', required=False, type=str, multiple=True)
@click.option('--plantit_token', required=False, type=str)
@click.option('--plantit_url', required=False, type=str)
def zip(
        input_dir,
        output_dir,
        name,
        max_size,
        include_patterns,
        include_names,
        exclude_patterns,
        exclude_names,
        plantit_token,
        plantit_url):
    commands.zip(
        input_dir=input_dir,
        output_dir=output_dir,
        name=name,
        max_size=max_size,
        include_patterns=include_patterns,
        include_names=include_names,
        exclude_patterns=exclude_patterns,
        exclude_names=exclude_names,
        plantit_url=plantit_url,
        plantit_token=plantit_token)


@click.command()
@click.argument('input_dir')
@click.option('--cyverse_token', required=True, type=str)
@click.option('-o', '--output_dir', required=False, type=str)
@click.option('-p', '--patterns', required=False, type=str, multiple=True)
@click.option('--overwrite', required=False, type=str, multiple=True)
@click.option('--plantit_token', required=False, type=str)
@click.option('--plantit_url', required=False, type=str)
def pull(
        input_dir,
        cyverse_token,
        output_dir,
        patterns,
        overwrite,
        plantit_token,
        plantit_url):
    commands.pull(
        remote_path=input_dir,
        local_path=output_dir,
        patterns=patterns,
        overwrite=overwrite,
        cyverse_token=cyverse_token,
        plantit_url=plantit_url,
        plantit_token=plantit_token)


@click.command()
@click.argument('output_dir')
@click.option('--cyverse_token', required=True, type=str)
@click.option('-i', '--input_dir', required=False, type=str)
@click.option('-ip', '--include_patterns', required=False, type=str, multiple=True)
@click.option('-in', '--include_names', required=False, type=str, multiple=True)
@click.option('-ep', '--exclude_patterns', required=False, type=str, multiple=True)
@click.option('-en', '--exclude_names', required=False, type=str, multiple=True)
@click.option('--plantit_token', required=False, type=str)
@click.option('--plantit_url', required=False, type=str)
def push(
        output_dir,
        cyverse_token,
        input_dir,
        include_patterns,
        include_names,
        exclude_patterns,
        exclude_names,
        plantit_token,
        plantit_url):
    commands.push(
        local_path=input_dir,
        remote_path=output_dir,
        cyverse_token=cyverse_token,
        include_patterns=include_patterns,
        include_names=include_names,
        exclude_patterns=exclude_patterns,
        exclude_names=exclude_names,
        plantit_url=plantit_url,
        plantit_token=plantit_token)


@click.command()
@click.argument('flow')
@click.option('--plantit_token', required=False, type=str)
@click.option('--plantit_url', required=False, type=str)
@click.option('--docker_username', required=False, type=str)
@click.option('--docker_password', required=False, type=str)
def run(flow,
        plantit_token,
        plantit_url,
        docker_username,
        docker_password):
    with open(flow, 'r') as file:
        errors, options = parse_options(yaml.safe_load(file))
        if len(errors) > 0:
            raise ValueError(f"Invalid configuration: {', '.join(errors[1])}")

        commands.run(
            options=options,
            plantit_url=plantit_url,
            plantit_token=plantit_token,
            docker_username=docker_username,
            docker_password=docker_password)
