from dataclasses import dataclass
from pathlib import Path


@dataclass
class SubmitConfig:
    script: Path
    host: str
    port: int
    username: str
    password: str
    key: str
    timeout: int = 10
    allow_stderr: bool = False
