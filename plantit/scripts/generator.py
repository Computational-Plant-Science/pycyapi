import logging
import dataclasses
from datetime import timedelta
from math import ceil
from os import environ
from os.path import join
from typing import List, Tuple, Optional

from _warnings import warn

from plantit import docker
from plantit.scripts.models import BindMount, EnvironmentVariable, ScriptConfig
from plantit.slurm import SLURM_TEMPLATE
from plantit.terrain.clients import TerrainClient


def get_log_file_name(config: ScriptConfig):
    return f"plantit.{config.guid}"


class ScriptGenerator:
    def __init__(self, config: ScriptConfig):
        valid, validation_errors = ScriptGenerator.validate_config(config)
        if not valid:
            raise ValueError(f"Invalid config: {validation_errors}")

        inputs = ScriptGenerator.list_input_files(config)
        if len(inputs) == 0 and config.source:
            warn(f"Source configured ({config.source}) but no inputs found")

        self.logger = logging.getLogger(__name__)
        self.config = config
        self.inputs = inputs

    @staticmethod
    def validate_config(config: ScriptConfig) -> Tuple[bool, List[str]]:
        errors = []

        # check required attributes
        fields = dataclasses.fields(ScriptConfig)
        ftypes = {f.name: f.type for f in fields}
        missing = [f for f, t in ftypes if not getattr(config, f) and not t == Optional[t]]
        errors.append(f"Missing required fields: {', '.join(missing)}")

        # check attribute types
        for f, t in ftypes:
            v = getattr(config, f)
            tv = type(v)
            if not isinstance(tv, t):
                errors.append(f"Field {f} expected {t}, got {tv}")

        # check image is on DockerHub
        image_owner, image_name, image_tag = docker.parse_image_components(config.image)
        if not docker.image_exists(image_owner, image_name, image_tag):
            errors.append(f"Image {config.image} not found on Docker Hub")

        if config.source or config.sink:
            # TODO check source/sink exist in CyVerse data store
            pass

        return len(errors) == 0, errors

    @staticmethod
    def list_input_files(config: ScriptConfig) -> List[str]:
        pass

    @staticmethod
    def get_walltime(config: ScriptConfig):
        # if a time limit was requested at submission time, use that
        if config.walltime is not None:
            # otherwise use the default time limit
            spl = config.walltime.split(":")
            hours = int(spl[0])
            minutes = int(spl[1])
            seconds = int(spl[2])
            requested = timedelta(hours=hours, minutes=minutes, seconds=seconds)

        # round to the nearest hour, making sure not to exceed agent's maximum, then convert to HH:mm:ss string
        hours = ceil(requested.total_seconds() / 60 / 60)
        hours = (
            "1" if hours == 0 else f"{hours}"
        )  # if we rounded down to zero, bump to 1
        if len(hours) == 1:
            hours = f"0{hours}"
        walltime = f"{hours}:00:00"
        return walltime

    def gen_pull_headers(self) -> List[str]:
        headers = []

        # memory
        if "mem" not in self.config.header_skip:
            headers.append(f"#SBATCH --mem=1GB")

        # walltime
        headers.append(
            f"#SBATCH --time=00:30:00"
        )  # TODO: calculate as a function of input size?

        # queue
        queue = self.config.queue
        headers.append(f"#SBATCH --partition={queue}")

        # project/allocation
        if self.config.project is not None and self.config.project != "":
            headers.append(f"#SBATCH -A {self.config.project}")

        # nodes
        headers.append(f"#SBATCH -N 1")

        # cores
        headers.append(f"#SBATCH -n 1")

        # email notifications
        headers.append("#SBATCH --mail-type=END,FAIL")
        headers.append(f"#SBATCH --mail-user={self.config.email}")

        # log files
        headers.append("#SBATCH --output=plantit.%j.pull.out")
        headers.append("#SBATCH --error=plantit.%j.pull.err")

        return headers

    def gen_push_headers(self) -> List[str]:
        headers = []

        # memory
        if "--mem" not in self.config.header_skip:
            headers.append(f"#SBATCH --mem=1GB")

        # walltime
        # TODO: calculate as a function of number/size of output files?
        headers.append(f"#SBATCH --time=02:00:00")

        # queue
        queue = self.config.queue
        headers.append(f"#SBATCH --partition={queue}")

        # project/allocation
        if self.config.project is not None and self.config.project != "":
            headers.append(f"#SBATCH -A {self.config.project}")

        # nodes
        headers.append(f"#SBATCH -N 1")

        # cores
        headers.append(f"#SBATCH -n 1")

        # email notifications
        headers.append("#SBATCH --mail-type=END,FAIL")
        headers.append(f"#SBATCH --mail-user={self.config.email}")

        # log files
        headers.append("#SBATCH --output=plantit.%j.push.out")
        headers.append("#SBATCH --error=plantit.%j.push.err")

        return headers

    def gen_job_headers(self) -> List[str]:
        headers = []

        # memory
        if "mem" in self.config:
            headers.append(f"#SBATCH --mem={str(self.config.mem)}GB")

        # walltime
        if "walltime" in self.config:
            walltime = ScriptGenerator.get_walltime(self.config)
            # task.job_requested_walltime = walltime
            # task.save()
            headers.append(f"#SBATCH --time={walltime}")

        # queue/partition
        # if task.agent.orchestrator_queue is not None and task.agent.orchestrator_queue != '':
        #     headers.append(f"#SBATCH --partition={task.agent.orchestrator_queue}")
        # else:
        #     headers.append(f"#SBATCH --partition={task.agent.queue}")
        headers.append(f"#SBATCH --partition={self.config.queue}")

        # allocation
        if self.config.project is not None and self.config.project != "":
            headers.append(f"#SBATCH -A {self.config.project}")

        # cores per task
        if "cores" in self.config:
            headers.append(f"#SBATCH -c {int(self.config.cores)}")

        # nodes & tasks per node
        if len(self.config.input) > 0 and self.config.input.kind == "files":
            nodes = self.config.nodes
            tasks = min(len(self.inputs), self.config.tasks)
            # if task.agent.job_array: headers.append(f"#SBATCH --array=1-{len(inputs)}")
            headers.append(f"#SBATCH -N {nodes}")
            headers.append(f"#SBATCH --ntasks={tasks}")
        else:
            headers.append("#SBATCH -N 1")
            headers.append("#SBATCH --ntasks=1")

        # gpus
        if self.config.gpus:
            headers.append(f"#SBATCH --gres=gpu:1")

        # email notifications
        headers.append("#SBATCH --mail-type=END,FAIL")
        headers.append(f"#SBATCH --mail-user={self.config.email}")

        # log files
        headers.append("#SBATCH --output=plantit.%j.out")
        headers.append("#SBATCH --error=plantit.%j.err")

        newline = "\n"
        self.logger.debug(f"Using headers: {newline.join(headers)}")
        return headers

    def gen_pull_command(self) -> List[str]:
        commands = []

        # make sure we have inputs
        if not self.inputs:
            return commands

        # job arrays may cause an invalid singularity cache due to lots of simultaneous pulls of the same image...
        # just pull it once ahead of time so it's already cached
        # TODO: set the image path to the cached one
        workflow_image = self.config.image
        workflow_shell = self.config.shell
        if workflow_shell is None:
            workflow_shell = "sh"
        pull_image_command = f"singularity exec {workflow_image} {workflow_shell} -c 'echo \"refreshing {workflow_image}\"'"
        commands.append(pull_image_command)

        # singularity must be pre-authenticated on the agent, e.g. with `singularity remote login --username <your username> docker://docker.io`
        # also, if this is a job array, all jobs will invoke iget, but files will only be downloaded once (since we don't use -f for force)
        workdir = join(self.config.workdir, self.config.workdir, "input")
        icommands_image = f"docker://computationalplantscience/icommands"
        pull_data_command = (
            f"singularity exec {icommands_image} iget -r {self.config.source} {workdir}"
        )
        commands.append(pull_data_command)

        newline = "\n"
        self.logger.debug(f"Using pull command: {newline.join(commands)}")
        return commands

    def gen_push_command(self) -> List[str]:
        commands = []

        # create staging directory
        staging_dir = f"{self.config.guid}_staging"
        mkdir_command = f"mkdir -p {staging_dir}"
        commands.append(mkdir_command)

        # create zip directory
        zip_dir = f"{self.config.guid}_zip"
        mkdir_command = f"mkdir -p {zip_dir}"
        commands.append(mkdir_command)

        # move results into staging and zip directories
        mv_zip_dir_command = f"mv -t {zip_dir} "
        output = self.config.output
        if "include" in output:
            if "names" in output["include"]:
                for name in output["include"]["names"]:
                    commands.append(f"cp {name} {join(staging_dir, name)}")
                    mv_zip_dir_command = mv_zip_dir_command + f"{name} "
            if "patterns" in output["include"]:
                for pattern in list(output["include"]["patterns"]):
                    commands.append(f"cp *.{pattern} {staging_dir}/")
                    mv_zip_dir_command = mv_zip_dir_command + f"*.{pattern} "
                # include all scheduler log files in zip file
                for pattern in ["out", "err"]:
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
        zip_name = f"{self.config.guid}.zip"
        zip_path = join(staging_dir, zip_name)
        zip_command = f"zip -r {zip_path} {zip_dir}/*"
        commands.append(zip_command)

        # transfer contents of staging dir to CyVerse
        to_path = output["to"]
        image = f"docker://computationalplantscience/icommands"
        # force = output['force']
        force = False
        # just_zip = output['just_zip']
        just_zip = False
        # push_command = f"singularity exec {image} iput -r{' -f ' if force else ' '}{staging_dir}{('/' + zip_name) if just_zip else '/*'} {to_path}/"
        push_command = f"singularity exec {image} iput -f {staging_dir}{('/' + zip_name) if just_zip else '/*'} {to_path}/"
        commands.append(push_command)

        newline = "\n"
        self.logger.debug(f"Using push commands: {newline.join(commands)}")
        return commands

    def gen_job_command(self) -> List[str]:
        commands = []

        # if this agent uses TACC's launcher, use a parameter sweep script
        if self.config.launcher:
            commands.append(
                f"export LAUNCHER_WORKDIR={join(self.__task.agent.workdir, self.__task.workdir)}"
            )
            commands.append(
                f"export LAUNCHER_JOB_FILE={environ.get('LAUNCHER_SCRIPT_NAME')}"
            )
            commands.append("$LAUNCHER_DIR/paramrun")
        # otherwise use SLURM job arrays
        else:
            if self.inputs:
                if len(self.inputs) > 1 and self.config.parallel:
                    input_path = join(self.config.workdir, "input", "$file")
                    commands.append(
                        f"file=$(head -n $SLURM_ARRAY_TASK_ID inputs.list | tail -1)"
                    )

            commands = commands + ScriptGenerator.gen_singularity_invocation(
                work_dir=self.config.workdir,
                image=self.config.image,
                commands=self.config.command,
                env=self.config.environment,
                bind_mounts=self.config.bind_mounts,
                no_cache=self.config.no_cache,
                gpus=self.config.gpus,
                shell=self.config.shell,
            )

        newline = "\n"
        self.logger.debug(f"Using container commands: {newline.join(commands)}")
        return commands

    def gen_pull_script(self) -> List[str]:
        template = [line for line in SLURM_TEMPLATE.splitlines()]
        headers = self.gen_pull_headers()
        command = self.gen_pull_command()
        return template + headers + command

    def gen_push_script(self) -> List[str]:
        template = [line for line in SLURM_TEMPLATE.splitlines()]
        headers = self.gen_push_headers()
        command = self.gen_push_command()
        return template + headers + command

    def gen_job_script(self) -> List[str]:
        template = [line for line in SLURM_TEMPLATE.splitlines()]
        headers = self.gen_job_headers()
        command = self.gen_job_command()
        return template + headers + command

    def gen_launcher_script(self) -> List[str]:
        lines: List[str] = []
        if "input" in self.config:
            input_kind = self.config.input.kind
            input_path = self.config.input.path
            input_name = input_path.rpartition("/")[2]
            client = TerrainClient(self.config.token)
            input_list = (
                []
                if ("input" not in self.config or not self.config.input)
                else [client.stat(input_path)["path"].rpartition("/")[2]]
                if self.config.parallel
                else [f["label"] for f in self.client.list_files(input_path)]
            )

            if input_kind == "files":
                for i, file_name in enumerate(input_list):
                    path = join(self.config.workdir, "input", input_name, file_name)
                    lines = lines + ScriptGenerator.gen_singularity_invocation(
                        work_dir=self.config.work_dir,
                        image=self.config.image,
                        commands=self.config.command,
                        env=self.config.env,
                        bind_mounts=self.config.bind_mounts,
                        no_cache=self.config.no_cache,
                        gpus=self.config.gpus,
                        shell=self.config.shell,
                        index=i,
                    )
                return lines
            elif input_kind == "directory":
                path = join(self.config.workdir, "input", input_name)
            elif input_kind == "file":
                path = join(self.config.workdir, "input", self.inputs[0])
            else:
                raise ValueError(f"Unsupported 'input.kind': {input_kind}")
        elif self.config.iterations > 1:
            iterations = self.config.iterations
            for i in range(0, iterations):
                lines = lines + ScriptGenerator.gen_singularity_invocation(
                    work_dir=self.config.work_dir,
                    image=self.config.image,
                    commands=self.config.command,
                    env=self.config.env,
                    bind_mounts=self.config.bind_mounts,
                    no_cache=self.config.no_cache,
                    gpus=self.config.gpus,
                    shell=self.config.shell,
                    index=i,
                )
            return lines

        return lines + ScriptGenerator.gen_singularity_invocation(
            work_dir=self.config.work_dir,
            image=self.config.image,
            commands=self.config.command,
            env=self.config.env,
            bind_mounts=self.config.bind_mounts,
            no_cache=self.config.no_cache,
            gpus=self.config.gpus,
            shell=self.config.shell,
        )

    @staticmethod
    def gen_singularity_invocation(
        work_dir: str,
        image: str,
        commands: str,
        env: List[EnvironmentVariable] = None,
        bind_mounts: List[BindMount] = None,
        no_cache: bool = False,
        gpus: int = 0,
        shell: str = None,
        docker_username: str = None,
        docker_password: str = None,
    ) -> List[str]:
        command = ""

        # prepend environment variables in SINGULARITYENV_<key> format
        if env is not None:
            if len(env) > 0:
                command += " ".join(
                    [
                        f"SINGULARITYENV_{v['key'].upper().replace(' ', '_')}=\"{v['value']}\""
                        for v in env
                    ]
                )
            command += " "

        # singularity invocation and working directory
        command += f" singularity exec --home {work_dir}"

        # add bind mount arguments
        if bind_mounts is not None and len(bind_mounts) > 0:
            command += " --bind " + ",".join(
                [bm.format(work_dir) for bm in bind_mounts]
            )

        # whether to use the Singularity cache
        if no_cache:
            command += " --disable-cache"

        # whether to use GPUs (Nvidia)
        if gpus:
            command += " --nv"

        # append the command
        if shell is None:
            shell = "sh"
        command += f" {image} {shell} -c '{commands}'"

        # docker auth info (optional)
        if docker_username is not None and docker_password is not None:
            command = (
                f"SINGULARITY_DOCKER_USERNAME={docker_username} SINGULARITY_DOCKER_PASSWORD={docker_password} "
                + command
            )

        return [command]
