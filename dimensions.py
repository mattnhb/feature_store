from functools import reduce
from pprint import pprint

import pyspark.sql.functions as F
from operator import and_

from pyspark.sql import DataFrame

# columns = {
#     "subproduto": {"geral": "subproduto"},
#     "janela": {"ultimos_365_dias": "data_transacao"},
#     "periodo": {"geral": "data_transacao"},
# }


class DimensionsFactory:
    def create_columns(self, df: DataFrame, columns, vision) -> DataFrame:
        columns.update({"visao": {vision: "placeholder"}})
        return reduce(
            lambda acc_df, column: acc_df.withColumn(
                column, F.lit(next(iter(columns[column].keys())))
            ),
            columns.keys(),
            df,
        )
