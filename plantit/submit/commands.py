import logging

from plantit.submit.models import SubmitConfig
from plantit.submit.ssh import SSH
from plantit.submit.utils import clean_html, parse_job_id

logger = logging.getLogger(__name__)


def submit(config: SubmitConfig) -> str:
    with SSH(
        host=config.host,
        port=config.port,
        username=config.username,
        password=config.password,
        pkey=config.key,
        timeout=config.timeout,
    ) as client:
        command = f"sbatch {config.script}"
        logger.info(f"Submitting {config.script.name} on '{config.host}'")
        stdin, stdout, stderr = client.exec_command(
            f"bash --login -c '{command}'", get_pty=True
        )
        stdin.close()

        def read_stdout():
            for line in iter(lambda: stdout.readline(2048), ""):
                clean = clean_html(line).strip()
                logger.debug(f"Received stdout from '{config.host}': '{clean}'")
                yield clean

        def read_stderr():
            for line in iter(lambda: stderr.readline(2048), ""):
                clean = clean_html(line).strip()
                logger.warning(f"Received stderr from '{config.host}': '{clean}'")
                yield clean

        output = [line for line in read_stdout()]
        errors = [line for line in read_stderr()]

        if stdout.channel.recv_exit_status() != 0:
            raise Exception(f"Received non-zero exit status from '{config.host}'")
        elif not config.allow_stderr and len(errors) > 0:
            raise Exception(f"Received stderr: {errors}")

        job_id = parse_job_id(output[-1])
        return job_id
