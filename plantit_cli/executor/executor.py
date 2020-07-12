from abc import ABC, abstractmethod

from plantit_cli.run import Run


class Executor(ABC):
    """
    Workflow execution engine.
    """

    @property
    @abstractmethod
    def name(self):
        pass

    @abstractmethod
    def execute(self, pipeline: Run):
        pass
