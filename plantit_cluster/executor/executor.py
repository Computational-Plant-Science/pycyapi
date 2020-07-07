from abc import ABC, abstractmethod

from plantit_cluster.pipeline import Pipeline


class Executor(ABC):
    """
    Pipeline execution engine.
    """

    @property
    @abstractmethod
    def name(self):
        pass

    @abstractmethod
    def execute(self, pipeline: Pipeline):
        pass
