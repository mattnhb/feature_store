from abc import ABC, abstractmethod


class BaseOriginData(ABC):
    @abstractmethod
    def load_data(self):
        pass
