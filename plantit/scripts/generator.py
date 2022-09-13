import dataclasses
import logging
from datetime import timedelta
from math import ceil
from os import environ, linesep
from os.path import join
from pathlib import Path
from typing import List, Optional, Tuple

from _warnings import warn

from plantit import docker
from plantit.cyverse.clients import CyverseClient
from plantit.scripts.models import (
    BindMount,
    EnvironmentVariable,
    ParallelStrategy,
    ScriptConfig,
    Shell, JobqueueConfig,
)

SHEBANG = "#!/bin/bash"
LAUNCHER_SCRIPT_NAME = "launch"  # TODO make configurable
ICOMMANDS_IMAGE = f"docker://computationalplantscience/icommands"
PLANTIT_IMAGE = f"docker://computationalplantscience/plantit"


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
        missing = [
            f for f, t in ftypes.items() if getattr(config, f) is None and t != Optional[t] and t != JobqueueConfig
        ]
        if len(missing) > 0:
            errors.append(f"Missing required fields: {', '.join(missing)}")

        # validate jobqueue config
        if not config.jobqueue.queue:
            errors.append(f"Cluster queue must be provided")

        # check image is on DockerHub
        image_owner, image_name, image_tag = docker.parse_image_components(config.image)
        if not docker.image_exists(image_name, owner=image_owner, tag=image_tag):
            errors.append(f"Image {config.image} not found on Docker Hub")

        client = None
        if config.source:
            client = CyverseClient(config.token)
            if not client.exists(config.source):
                errors.append(f"Source {config.source} not found in CyVerse data store")

        if config.sink:
            client = client if client else CyverseClient(config.token)
            if not client.exists(config.sink):
                errors.append(f"Sink {config.sink} not found in CyVerse data store")

        return len(errors) == 0, errors

    @staticmethod
    def list_input_files(config: ScriptConfig) -> List[Path]:
        if config.input:
            return [p for p in Path(config.input).glob("*")]
        else:
            return []

    @staticmethod
    def get_job_walltime(config: ScriptConfig):
        if config.jobqueue.walltime is None:
            walltime = timedelta(hours=1)
        else:
            s = config.jobqueue.walltime.split(":")
            hours, minutes, seconds = int(s[0]), int(s[1]), int(s[2])
            walltime = timedelta(hours=hours, minutes=minutes, seconds=seconds)

        # round to nearest hour and convert to HH:mm:ss
        hours = ceil(walltime.total_seconds() / 60 / 60)
        hours = (
            "1" if hours == 0 else f"{hours}"
        )  # if we rounded down to zero, bump to 1
        if len(hours) == 1:
            hours = f"0{hours}"
        return f"{hours}:00:00"

    def gen_pull_headers(self) -> List[str]:
        headers = [f"#SBATCH --job-name=plantit-pull-{self.config.guid}"]
        if self.config.jobqueue.project:
            headers.append(f"#SBATCH -A {self.config.jobqueue.project}")
        headers.append(f"#SBATCH --partition={self.config.jobqueue.queue}")
        headers.append("#SBATCH --mail-type=END,FAIL")
        headers.append(f"#SBATCH --mail-user={self.config.email}")
        headers.append("#SBATCH --output=plantit.%j.pull.out")
        headers.append("#SBATCH --error=plantit.%j.pull.err")
        headers.append(f"#SBATCH -N 1")
        headers.append(f"#SBATCH -n 1")
        headers.append(
            f"#SBATCH --time=01:00:00"
        )  # TODO: calculate as a function of input size
        if "mem" not in self.config.jobqueue.header_skip:
            headers.append(f"#SBATCH --mem=1GB")

        self.logger.debug(f"Using pull headers: {linesep.join(headers)}")
        return headers

    def gen_push_headers(self) -> List[str]:
        headers = [f"#SBATCH --job-name=plantit-push-{self.config.guid}"]
        if self.config.jobqueue.project:
            headers.append(f"#SBATCH -A {self.config.jobqueue.project}")
        headers.append(f"#SBATCH --partition={self.config.jobqueue.queue}")
        headers.append("#SBATCH --mail-type=END,FAIL")
        headers.append(f"#SBATCH --mail-user={self.config.email}")
        headers.append("#SBATCH --output=plantit.%j.push.out")
        headers.append("#SBATCH --error=plantit.%j.push.err")
        headers.append(f"#SBATCH -N 1")
        headers.append(f"#SBATCH -n 1")
        headers.append(f"#SBATCH --time=01:00:00")  # TODO: make configurable?
        if "--mem" not in self.config.jobqueue.header_skip:
            headers.append(f"#SBATCH --mem=1GB")

        self.logger.debug(f"Using push headers: {linesep.join(headers)}")
        return headers

    def gen_job_headers(self) -> List[str]:
        headers = [f"#SBATCH --job-name=plantit-run-{self.config.guid}"]
        headers.append("#SBATCH --mail-type=END,FAIL")
        headers.append(f"#SBATCH --mail-user={self.config.email}")
        headers.append("#SBATCH --output=plantit.%j.out")
        headers.append("#SBATCH --error=plantit.%j.err")
        if self.config.jobqueue.project:
            headers.append(f"#SBATCH -A {self.config.jobqueue.project}")
        headers.append(f"#SBATCH --partition={self.config.jobqueue.queue}")
        headers.append(f"#SBATCH -c {int(self.config.jobqueue.cores)}")
        headers.append(f"#SBATCH -N {self.config.jobqueue.nodes}")
        headers.append(f"#SBATCH --ntasks={self.config.jobqueue.tasks}")
        headers.append(
            f"#SBATCH --time={ScriptGenerator.get_job_walltime(self.config)}"
        )
        if self.config.gpus:
            headers.append(f"#SBATCH --gres=gpu:1")
        if not self.config.jobqueue.header_skip or "--mem" not in self.config.jobqueue.header_skip:
            headers.append(f"#SBATCH --mem={str(self.config.jobqueue.mem)}GB")

        self.logger.debug(f"Using run headers: {linesep.join(headers)}")
        return headers

    def gen_pull_command(self, icommands: bool = False) -> List[str]:
        if not self.config.source:
            return []

        commands = []

        # job arrays may cause an invalid singularity cache due to lots of simultaneous pulls of the same image...
        # just pull it once ahead of time so it's already cached
        # TODO: set the image path to the cached one
        image = self.config.image
        shell = self.config.shell
        if not shell:
            shell = "sh"
        commands.append(
            f"singularity exec {image} {shell} -c 'echo \"refreshing {image}\"'"
        )

        # specified or default input path
        input_path = self.config.input if self.config.input else "input"

        if icommands:
            # singularity must be pre-authenticated on the agent, e.g. with `singularity remote login --username <your
            # username> docker://docker.io` also, if this is a job array, all jobs will invoke iget, but files will only
            # be downloaded once (since we don't use -f for force)
            commands.append(
                f"singularity exec {ICOMMANDS_IMAGE} iget -r {self.config.source} {input_path}"
            )
        else:
            commands.append(
                f"singularity exec {PLANTIT_IMAGE} plantit pull -t {self.config.token} {self.config.source} -p {input_path}"
            )

        self.logger.debug(f"Using pull command: {linesep.join(commands)}")
        return commands

    def gen_push_command(self, icommands: bool = False) -> List[str]:
        if not self.config.sink:
            return []

        if icommands:
            # create zip dir
            zip_dir = f"{self.config.guid}_zip"
            commands = [f"mkdir -p {zip_dir}"]

            # copy results to zip dir
            commands.append(
                f'for i in {self.config.output}; do [ -f "$i" ] && cp -a "$i" {zip_dir}; done'
            )

            # zip results dir
            zip_name = f"{self.config.guid}.zip"
            commands.append(f"zip -r {zip_name} {zip_dir}/*")

            # push results to CyVerse and unzip once uploaded
            commands.append(
                f"singularity exec {ICOMMANDS_IMAGE} iput -Dzip -f {zip_name} {self.config.sink}/"
            )
            commands.append(
                f"singularity exec {ICOMMANDS_IMAGE} ibun -x {self.config.sink}/{zip_name} {self.config.sink}"
            )
        else:
            commands = [
                f"singularity exec {PLANTIT_IMAGE} push -t {self.config.token} {self.config.sink} -p {self.config.output}"
            ]

        self.logger.debug(f"Using push commands: {linesep.join(commands)}")
        return commands

    def gen_job_command(self) -> List[str]:
        if self.config.jobqueue.parallel == ParallelStrategy.LAUNCHER:
            commands = self.gen_launcher_entrypoint()
        elif self.config.jobqueue.parallel == ParallelStrategy.JOB_ARRAY:
            commands = self.gen_job_array_script()
        else:
            raise ValueError(
                f"Unsupported parallelism strategy {self.config.jobqueue.parallel}"
            )

        self.logger.debug(f"Using run commands: {linesep.join(commands)}")
        return commands

    def gen_pull_script(self) -> List[str]:
        headers = self.gen_pull_headers()
        command = self.gen_pull_command()
        return [SHEBANG] + headers + command

    def gen_push_script(self) -> List[str]:
        headers = self.gen_push_headers()
        command = self.gen_push_command()
        return [SHEBANG] + headers + command

    def gen_job_script(self) -> List[str]:
        headers = self.gen_job_headers()
        command = self.gen_job_command()
        return [SHEBANG] + headers + command

    def gen_job_array_script(self) -> List[str]:
        commands = []
        if self.inputs:
            if len(self.inputs) > 1:
                input_path = join(
                    self.config.workdir,
                    self.config.input if self.config.input else "input",
                    "$file",
                )
                commands.append(
                    f"file=$(head -n $SLURM_ARRAY_TASK_ID inputs.list | tail -1)"
                )

        commands = commands + ScriptGenerator.gen_singularity_invocation(
            work_dir=self.config.workdir,
            image=self.config.image,
            commands=self.config.entrypoint,
            env=self.config.environment,
            bind_mounts=self.config.bind_mounts,
            no_cache=self.config.no_cache,
            gpus=self.config.gpus,
            shell=self.config.shell,
        )
        return commands

    def gen_launcher_entrypoint(self) -> List[str]:
        commands = []
        commands.append(f"export LAUNCHER_WORKDIR={self.config.workdir}")
        commands.append(f"export LAUNCHER_JOB_FILE={environ.get(LAUNCHER_SCRIPT_NAME)}")
        commands.append("$LAUNCHER_DIR/paramrun")
        return commands

    def gen_launcher_script(self) -> List[str]:
        lines: List[str] = []
        if "input" in self.config:
            input_kind = self.config.input
            input_path = self.config.input.path
            input_name = input_path.rpartition("/")[2]
            client = CyverseClient(self.config.token)
            input_list = (
                []
                if ("input" not in self.config or not self.config.input)
                else [client.stat(input_path)["path"].rpartition("/")[2]]
                if self.config.jobqueue.parallel
                else [f["label"] for f in self.client.list_files(input_path)]
            )

            if input_kind == "files":
                for i, file_name in enumerate(input_list):
                    path = join(self.config.workdir, "input", input_name, file_name)
                    lines = lines + ScriptGenerator.gen_singularity_invocation(
                        work_dir=self.config.workdir,
                        image=self.config.image,
                        commands=self.config.entrypoint,
                        env=self.config.environment,
                        bind_mounts=self.config.bind_mounts,
                        no_cache=self.config.no_cache,
                        gpus=self.config.gpus,
                        shell=self.config.shell,
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
                    work_dir=self.config.workdir,
                    image=self.config.image,
                    commands=self.config.entrypoint,
                    env=self.config.environment,
                    bind_mounts=self.config.bind_mounts,
                    no_cache=self.config.no_cache,
                    gpus=self.config.gpus,
                    shell=self.config.shell,
                )
            return lines

        return lines + ScriptGenerator.gen_singularity_invocation(
            work_dir=self.config.workdir,
            image=self.config.image,
            commands=self.config.entrypoint,
            env=self.config.environment,
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
        shell: Shell = None,
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
        command += f"singularity exec --home {work_dir}"

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
        command += f" {image} {shell.value} -c \"{commands}\""

        # docker auth info (optional)
        if docker_username is not None and docker_password is not None:
            command = (
                f"SINGULARITY_DOCKER_USERNAME={docker_username} SINGULARITY_DOCKER_PASSWORD={docker_password} "
                + command
            )

        return [command]
