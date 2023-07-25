from functools import reduce
from pprint import pprint

import pyspark.sql.functions as F
from operator import and_

from pyspark.sql import DataFrame


class DimensionsFactory:
    def create_columns(self, df: DataFrame, columns, vision: str) -> DataFrame:
        columns.update({"visao": {vision: "placeholder"}})
        return reduce(
            lambda acc_df, column: acc_df.withColumn(
                column, F.lit(next(iter(columns[column].keys())))
            ),
            columns.keys(),
            df,
        )
