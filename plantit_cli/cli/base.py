import click

from plantit_cli import commands


@click.group()
def cli():
    pass


@cli.group()
def terrain():
    pass


@cli.group()
def irods():
    pass


@cli.group()
def dask():
    pass


@cli.group()
def launcher():
    pass


@cli.command()
def ping():
    print('pong')


@cli.command()
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
def zip(input_dir,
        output_dir,
        name,
        # max_size,
        include_pattern,
        include_name,
        exclude_pattern,
        exclude_name):
    commands.zip(
        input_dir=input_dir,
        output_dir=output_dir,
        name=name,
        # max_size=max_size,
        include_patterns=include_pattern,
        include_names=include_name,
        exclude_patterns=exclude_pattern,
        exclude_names=exclude_name)


if __name__ == '__main__':
    cli()
