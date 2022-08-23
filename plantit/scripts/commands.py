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
    headers = []

    # memory
    if not has_virtual_memory(self.__task.agent):
        headers.append(f"#SBATCH --mem=1GB")

    # walltime
    headers.append(f"#SBATCH --time=00:30:00")  # TODO: calculate as a function of input size?

    # queue
    queue = config.queue
    headers.append(f"#SBATCH --partition={queue}")

    # project/allocation
    if config.project is not None and config.project != '':
        headers.append(f"#SBATCH -A {self.__task.agent.project}")

    # nodes
    headers.append(f"#SBATCH -N 1")

    # cores
    headers.append(f"#SBATCH -n 1")

    # email notifications
    headers.append("#SBATCH --mail-type=END,FAIL")
    headers.append(f"#SBATCH --mail-user={config.email}")

    # log files
    headers.append("#SBATCH --output=plantit.%j.pull.out")
    headers.append("#SBATCH --error=plantit.%j.pull.err")

    return headers


def gen_push_headers(config: ScriptConfig) -> List[str]:
    headers = []

    # memory
    if '--mem' not in config.header_skip
        headers.append(f"#SBATCH --mem=1GB")

    # walltime
    # TODO: calculate as a function of number/size of output files?
    headers.append(f"#SBATCH --time=02:00:00")

    # queue
    queue = config.queue
    headers.append(f"#SBATCH --partition={queue}")

    # project/allocation
    if config.project is not None and config.project != '':
        headers.append(f"#SBATCH -A {config.project}")

    # nodes
    headers.append(f"#SBATCH -N 1")

    # cores
    headers.append(f"#SBATCH -n 1")

    # email notifications
    headers.append("#SBATCH --mail-type=END,FAIL")
    headers.append(f"#SBATCH --mail-user={config.email}")

    # log files
    headers.append("#SBATCH --output=plantit.%j.push.out")
    headers.append("#SBATCH --error=plantit.%j.push.err")

    return headers


def gen_pull_command(config: ScriptConfig) -> List[str]:
    commands = []

    # job arrays may cause an invalid singularity cache due to lots of simultaneous pulls of the same image...
    # just pull it once ahead of time so it's already cached
    # TODO: set the image path to the cached one
    workflow_image = config.image
    workflow_shell = config.get('shell', None)
    if workflow_shell is None: workflow_shell = 'sh'
    pull_image_command = f"singularity exec {workflow_image} {workflow_shell} -c 'echo \"refreshing {workflow_image}\"'"
    commands.append(pull_image_command)

    # make sure we have inputs
    if 'input' not in config: return commands
    input = config.input
    if input is None: return []

    # singularity must be pre-authenticated on the agent, e.g. with `singularity remote login --username <your username> docker://docker.io`
    # also, if this is a job array, all jobs will invoke iget, but files will only be downloaded once (since we don't use -f for force)
    input_path = input['path']
    workdir = join(config.workdir, config.workdir, 'input')
    icommands_image = f"docker://computationalplantscience/icommands"
    pull_data_command = f"singularity exec {icommands_image} iget -r {input_path} {workdir}"
    commands.append(pull_data_command)

    newline = '\n'
    logger.debug(f"Using pull command: {newline.join(commands)}")
    return commands


def gen_push_command(config: ScriptConfig) -> List[str]:
    commands = []

    # create staging directory
    staging_dir = f"{config.guid}_staging"
    mkdir_command = f"mkdir -p {staging_dir}"
    commands.append(mkdir_command)

    # create zip directory
    zip_dir = f"{config.guid}_zip"
    mkdir_command = f"mkdir -p {zip_dir}"
    commands.append(mkdir_command)

    # move results into staging and zip directories
    mv_zip_dir_command = f"mv -t {zip_dir} "
    output = config.output
    if 'include' in output:
        if 'names' in output['include']:
            for name in output['include']['names']:
                commands.append(f"cp {name} {join(staging_dir, name)}")
                mv_zip_dir_command = mv_zip_dir_command + f"{name} "
        if 'patterns' in output['include']:
            for pattern in (list(output['include']['patterns'])):
                commands.append(f"cp *.{pattern} {staging_dir}/")
                mv_zip_dir_command = mv_zip_dir_command + f"*.{pattern} "
            # include all scheduler log files in zip file
            for pattern in ['out', 'err']:
                mv_zip_dir_command = mv_zip_dir_command + f"*.{pattern} "
    else:
        raise ValueError(f"No output filenames & patterns to include")
    commands.append(mv_zip_dir_command)

    # filter unwanted results from staging directory
    # TODO: can we do this in a single step with mv?
    # rm_command = f"rm "
    # if 'exclude' in output:
    #     if 'patterns' in output['exclude']:
    #         command = command + ' ' + ' '.join(
    #             ['--exclude_pattern ' + pattern for pattern in output['exclude']['patterns']])
    #     if 'names' in output['exclude']:
    #         command = command + ' ' + ' '.join(
    #             ['--exclude_name ' + pattern for pattern in output['exclude']['names']])

    # zip results
    zip_name = f"{config.guid}.zip"
    zip_path = join(staging_dir, zip_name)
    zip_command = f"zip -r {zip_path} {zip_dir}/*"
    commands.append(zip_command)

    # transfer contents of staging dir to CyVerse
    to_path = output['to']
    image = f"docker://computationalplantscience/icommands"
    # force = output['force']
    force = False
    # just_zip = output['just_zip']
    just_zip = False
    # push_command = f"singularity exec {image} iput -r{' -f ' if force else ' '}{staging_dir}{('/' + zip_name) if just_zip else '/*'} {to_path}/"
    push_command = f"singularity exec {image} iput -f {staging_dir}{('/' + zip_name) if just_zip else '/*'} {to_path}/"
    commands.append(push_command)

    newline = '\n'
    logger.debug(f"Using push commands: {newline.join(commands)}")
    return commands


def gen_job_headers(config: ScriptConfig) -> List[str]:
    headers = []

    # memory
    if 'mem' in config:
        headers.append(f"#SBATCH --mem={str(mem)}GB")

    # walltime
    if 'walltime' in config:
        walltime = get_walltime(config)
        # task.job_requested_walltime = walltime
        # task.save()
        headers.append(f"#SBATCH --time={walltime}")

    # queue/partition
    # if task.agent.orchestrator_queue is not None and task.agent.orchestrator_queue != '':
    #     headers.append(f"#SBATCH --partition={task.agent.orchestrator_queue}")
    # else:
    #     headers.append(f"#SBATCH --partition={task.agent.queue}")
    headers.append(f"#SBATCH --partition={config.queue}")

    # allocation
    if config.project is not None and config.project != '':
        headers.append(f"#SBATCH -A {config.project}")

    # cores per task
    if 'cores' in config:
        headers.append(f"#SBATCH -c {int(config.cores)}")

    # nodes & tasks per node
    if len(inputs) > 0 and config.input.kind == 'files':
        nodes = config.nodes
        tasks = min(len(inputs), config.tasks)
        # if task.agent.job_array: headers.append(f"#SBATCH --array=1-{len(inputs)}")
        headers.append(f"#SBATCH -N {nodes}")
        headers.append(f"#SBATCH --ntasks={tasks}")
    else:
        headers.append("#SBATCH -N 1")
        headers.append("#SBATCH --ntasks=1")

    # gpus
    gpus = config.gpus if 'gpus' in config else 0
    if gpus:
        headers.append(f"#SBATCH --gres=gpu:{gpus}")

    # email notifications
    headers.append("#SBATCH --mail-type=END,FAIL")
    headers.append(f"#SBATCH --mail-user={config.email}")

    # log files
    headers.append("#SBATCH --output=plantit.%j.out")
    headers.append("#SBATCH --error=plantit.%j.err")

    newline = '\n'
    logger.debug(f"Using headers: {newline.join(headers)}")
    return headers


def gen_job_command(config: ScriptConfig) -> List[str]:
    commands = []

    # if this agent uses TACC's launcher, use a parameter sweep script
    if self.__task.agent.launcher:
        commands.append(f"export LAUNCHER_WORKDIR={join(self.__task.agent.workdir, self.__task.workdir)}")
        commands.append(f"export LAUNCHER_JOB_FILE={os.environ.get('LAUNCHER_SCRIPT_NAME')}")
        commands.append("$LAUNCHER_DIR/paramrun")
    # otherwise use SLURM job arrays
    else:
        options = self.__options
        work_dir = options['workdir']
        image = options['image']
        command = options['command']
        env = options['env']
        # TODO: if workflow is configured for gpu, use the number of gpus configured on the agent
        gpus = options['gpus'] if 'gpus' in options else 0
        parameters = (options['parameters'] if 'parameters' in options else []) + [
            Parameter(key='OUTPUT', value=options['output']['from']),
            Parameter(key='GPUS', value=str(gpus))]
        bind_mounts = options['bind_mounts'] if (
                    'mount' in options and isinstance(options['bind_mounts'], list)) else []
        no_cache = options['no_cache'] if 'no_cache' in options else False
        shell = options['shell'] if 'shell' in options else None

        if 'input' in options:
            input_kind = config.input.kind
            input_dir_name = config.input.path.rpartition('/')[2]

            if input_kind == 'files' or input_kind == 'file':
                input_path = join(config.workdir, 'input', input_dir_name,
                                  '$file') if input_kind == 'files' else join(config.workdir,
                                                                              'input', '$file')
                parameters = parameters + [Parameter(key='INPUT', value=input_path),
                                           Parameter(key='INDEX', value='$SLURM_ARRAY_TASK_ID')]
                commands.append(f"file=$(head -n $SLURM_ARRAY_TASK_ID inputs.list | tail -1)")
            elif config.input.kind == 'directory':
                input_path = join(config.workdir, 'input', input_dir_name)
                parameters = parameters + [Parameter(key='INPUT', value=input_path)]
            else:
                raise ValueError(f"Unsupported \'input.kind\': {input_kind}")

        commands = commands + gen_singularity_invocation(
            work_dir=work_dir,
            image=image,
            commands=command,
            env=env,
            parameters=parameters,
            bind_mounts=bind_mounts,
            no_cache=no_cache,
            gpus=gpus,
            shell=shell)

    newline = '\n'
    logger.debug(f"Using container commands: {newline.join(commands)}")
    return commands


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
