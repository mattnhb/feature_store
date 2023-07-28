from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


class BaseJsonifyHandler(ABC):
    @abstractmethod
    def to_dynamo_schema(self, df: DataFrame) -> DataFrame:
        pass
