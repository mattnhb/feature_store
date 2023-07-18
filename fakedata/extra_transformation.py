from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from base.extra_transformation import BaseExtraTransformer


class ExtraTransformer(BaseExtraTransformer):
    def apply_extra_transformation(self, df: DataFrame) -> DataFrame:
        # df = df.withColumn("dia_semana", F.dayofweek("data_transacao"))
        return df
