import re
import logging
from typing import List

import paramiko
from paramiko.ssh_exception import AuthenticationException, ChannelException, NoValidConnectionsError, SSHException
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

logger = logging.getLogger(__name__)


class SSH:
    """
    Wraps a paramiko client with either password or key authentication.
    Preserves context manager usability.
    """

    def __init__(self,
                 host: str,
                 port: int,
                 username: str,
                 password: str = None,
                 timeout: int = 10,
                 pkey: str = None):
        self.client = None
        self.logger = logging.getLogger(SSH.__name__)
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.pkey = pkey
        self.timeout = timeout

    def __enter__(self):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        if self.password is not None:
            client.connect(self.host, self.port, self.username, self.password, timeout=self.timeout)
        elif self.pkey is not None:
            key = paramiko.RSAKey.from_private_key_file(self.pkey)
            client.connect(hostname=self.host, port=self.port, username=self.username, pkey=key, timeout=self.timeout)
        else:
            raise ValueError(f"No authentication strategy provided")

        self.client = client
        return self.client

    def __exit__(self, exc_type, exc_value, traceback):
        self.client.close()