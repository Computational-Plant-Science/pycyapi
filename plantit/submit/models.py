from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class SubmitConfig:
    workdir: str
    script: str
    host: str
    port: int
    username: str
    password: Optional[str] = None
    key: Optional[str] = None
    timeout: int = 10
    allow_stderr: bool = False
