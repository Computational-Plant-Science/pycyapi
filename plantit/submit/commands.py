import logging
import re
import traceback

from plantit.submit.ssh import SSH

logger = logging.getLogger(__name__)


def clean_html(raw_html: str) -> str:
    expr = re.compile("<.*?>")
    text = re.sub(expr, "", raw_html)
    return text


def parse_job_id(line: str) -> str:
    try:
        return str(int(line.replace("Submitted batch job", "").strip()))
    except:
        raise Exception(
            f"Failed to parse job ID from '{line}'\n{traceback.format_exc()}"
        )


def submit(
    script,
    host,
    port,
    username,
    password,
    key,
    timeout: int = 10,
    allow_stderr: bool = False,
) -> str:
    with SSH(
        host=host,
        port=port,
        username=username,
        password=password,
        pkey=key,
        timeout=timeout,
    ) as ssh:
        command = f"sbatch {script}"
        logger.info(f"Submitting {script} on '{host}'")
        stdin, stdout, stderr = ssh.client.exec_command(
            f"bash --login -c '{command}'", get_pty=True
        )
        stdin.close()

        def read_stdout():
            for line in iter(lambda: stdout.readline(2048), ""):
                clean = clean_html(line).strip()
                logger.debug(f"Received stdout from '{ssh.host}': '{clean}'")
                yield clean

        def read_stderr():
            for line in iter(lambda: stderr.readline(2048), ""):
                clean = clean_html(line).strip()
                logger.warning(f"Received stderr from '{ssh.host}': '{clean}'")
                yield clean

        output = [line for line in read_stdout()]
        errors = [line for line in read_stderr()]

        if stdout.channel.recv_exit_status() != 0:
            raise Exception(f"Received non-zero exit status from '{host}'")
        elif not allow_stderr and len(errors) > 0:
            raise Exception(f"Received stderr: {errors}")

        job_id = parse_job_id(output[-1])
        return job_id
