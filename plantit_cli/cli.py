import click
import yaml

from plantit_cli.runner.runner import Runner
from plantit_cli.config import Config
from plantit_cli.store.terrain_store import TerrainStore
from plantit_cli.utils import validate_config


@click.command()
@click.argument('workflow')
@click.option('--plantit_token', required=False, type=str)
@click.option('--cyverse_token', required=False, type=str)
@click.option('--docker_username', required=False, type=str)
@click.option('--docker_password', required=False, type=str)
def run(workflow, plantit_token, cyverse_token, docker_username, docker_password):
    with open(workflow, 'r') as file:
        config_yaml = yaml.safe_load(file)
        config_yaml['plantit_token'] = plantit_token
        config_yaml['cyverse_token'] = cyverse_token
        config_yaml['docker_username'] = docker_username
        config_yaml['docker_password'] = docker_password

        if 'api_url' not in config_yaml:
            config_yaml['api_url'] = None

        if 'gpu' in config_yaml:
            del config_yaml['gpu']

        config = Config(**config_yaml)
        config_valid = validate_config(config)
        if type(config_valid) is not bool:
            raise ValueError(f"Invalid configuration: {', '.join(config_valid[1])}")

        store = TerrainStore(config)
        runner = Runner(store)
        runner.run(config)
