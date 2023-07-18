from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


class BaseExtraTransformer(ABC):
    @abstractmethod
    def apply_extra_transformation(self, df: DataFrame) -> DataFrame:
        pass
