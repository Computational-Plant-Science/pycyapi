from typing import List

from plantit.scripts.models import EnvironmentVariable, BindMount, Parameter, ScriptConfig
from plantit.terrain.clients import TerrainClient


def get_walltime(config: ScriptConfig):
    # if a time limit was requested at submission time, use that
    if config.walltime is not None:
        # otherwise use the default time limit
        spl = config.walltime.split(':')
        hours = int(spl[0])
        minutes = int(spl[1])
        seconds = int(spl[2])
        requested = timedelta(hours=hours, minutes=minutes, seconds=seconds)

    # round to the nearest hour, making sure not to exceed agent's maximum, then convert to HH:mm:ss string
    hours = ceil(requested.total_seconds() / 60 / 60)
    hours = '1' if hours == 0 else f"{hours}"  # if we rounded down to zero, bump to 1
    if len(hours) == 1: hours = f"0{hours}"
    walltime = f"{hours}:00:00"
    logger.info(f"Using walltime {walltime} for task {config.guid}")
    return walltime


SLURM_TEMPLATE = """
#!/bin/bash
#SBATCH --job-name=plantit
"""


def gen_pull_headers(config: ScriptConfig) -> List[str]:
    pass


def gen_push_headers(config: ScriptConfig) -> List[str]:
    pass


def gen_pull_command(config: ScriptConfig) -> List[str]:
    pass


def gen_push_command(config: ScriptConfig) -> List[str]:
    pass


def gen_job_headers(config: ScriptConfig) -> List[str]:
    pass


def gen_job_command(config: ScriptConfig) -> List[str]:
    pass


def gen_pull_script(config: ScriptConfig) -> List[str]:
    template = [line for line in SLURM_TEMPLATE.splitlines()]
    headers = gen_pull_headers(config)
    command = gen_pull_command(config)
    return template + \
           headers + \
           command


def gen_push_script(config: ScriptConfig) -> List[str]:
    template = [line for line in SLURM_TEMPLATE.splitlines()]
    headers = gen_push_headers(config)
    command = gen_push_command(config)
    return template + \
           headers + \
           command


def gen_job_script(config: ScriptConfig) -> List[str]:
    template = [line for line in SLURM_TEMPLATE.splitlines()]
    headers = gen_job_headers(config)
    command = gen_job_command(config)
    return template + \
           headers + \
           command


def gen_launcher_script(config: ScriptConfig) -> List[str]:
    lines: List[str] = []
    if 'input' in config:
        input_kind = config.input.kind
        input_path = config.input.path
        input_name = input_path.rpartition('/')[2]
        input_list = [] if ('input' not in config or not config.input) else \
            [TerrainClient(config.token).stat(input_path)['path'].rpartition('/')[2]] if kind == InputKind.FILE else [
                f['label'] for f in client.list_files(input_path)]


        if input_kind == 'files':
            for i, file_name in enumerate(input_list):
                path = join(config.workdir, 'input', input_name, file_name)
                lines = lines + TaskScripts.compose_singularity_invocation(
                    work_dir=config.work_dir,
                    image=config.image,
                    commands=config.command,
                    env=config.env,
                    bind_mounts=config.bind_mounts,
                    no_cache=config.no_cache,
                    gpus=config.gpus,
                    shell=config.shell,
                    index=i)
            return lines
        elif input_kind == 'directory':
            path = join(config.workdir, 'input', input_name)
        elif input_kind == 'file':
            path = join(config.workdir, 'input', inputs[0])
        else:
            raise ValueError(f"Unsupported \'input.kind\': {input_kind}")
    elif 'iterations' in config:
        iterations = config.iterations
        for i in range(0, iterations):
            lines = lines + TaskScripts.compose_singularity_invocation(
                work_dir=config.work_dir,
                image=config.image,
                commands=config.command,
                env=config.env,
                bind_mounts=config.bind_mounts,
                no_cache=config.no_cache,
                gpus=config.gpus,
                shell=config.shell,
                index=i)
        return lines

    return lines + gen_singularity_invocation(
        work_dir=config.work_dir,
        image=config.image,
        commands=config.command,
        env=config.env,
        bind_mounts=config.bind_mounts,
        no_cache=config.no_cache,
        gpus=config.gpus,
        shell=config.shell)


def gen_singularity_invocation(
        work_dir: str,
        image: str,
        commands: str,
        env: List[EnvironmentVariable] = None,
        bind_mounts: List[BindMount] = None,
        parameters: List[Parameter] = None,
        no_cache: bool = False,
        gpus: int = 0,
        shell: str = None,
        docker_username: str = None,
        docker_password: str = None,
        index: int = None) -> List[str]:
    command = ''

    # prepend environment variables in SINGULARITYENV_<key> format
    if env is not None:
        if len(env) > 0: command += ' '.join(
            [f"SINGULARITYENV_{v['key'].upper().replace(' ', '_')}=\"{v['value']}\"" for v in env])
        command += ' '

    # substitute parameters
    if parameters is None: parameters = []
    if index is not None: parameters.append(Parameter(key='INDEX', value=str(index)))
    parameters.append(Parameter(key='WORKDIR', value=work_dir))
    for parameter in parameters:
        key = parameter['key'].upper().replace(' ', '_')
        val = str(parameter['value'])
        command += f" SINGULARITYENV_{key}=\"{val}\""

    # singularity invocation and working directory
    command += f" singularity exec --home {work_dir}"

    # add bind mount arguments
    if bind_mounts is not None and len(bind_mounts) > 0:
        command += (' --bind ' + ','.join(
            [format_bind_mount(work_dir, mount_point) for mount_point in bind_mounts]))

    # whether to use the Singularity cache
    if no_cache: command += ' --disable-cache'

    # whether to use GPUs (Nvidia)
    if gpus: command += ' --nv'

    # append the command
    if shell is None: shell = 'sh'
    command += f" {image} {shell} -c '{commands}'"

    # don't want to reveal secrets, so log the command before prepending secret env vars
    logger.debug(f"Using command: '{command}'")

    # docker auth info (optional)
    if docker_username is not None and docker_password is not None:
        command = f"SINGULARITY_DOCKER_USERNAME={docker_username} SINGULARITY_DOCKER_PASSWORD={docker_password} " + command

    return [command]
