from functools import reduce
from pprint import pprint
from operator import and_
from pyspark.sql import DataFrame
from itertools import product

from dimensions import DimensionsFactory
from metrics import MetricsFactory
from predicates import PredicateFactory
from union_frames import UnionFrames

pf = PredicateFactory()
dimf = DimensionsFactory()
mf = MetricsFactory()

# d0 = (
#     (
#         df
#         .where(fabrica predicados)
#         .groupBy(*vision.get("grouped_by)
#         .agg(
#             fabrica metricas
#         )
#         .agg(
#                 F.count("valor").alias("count"),
#                 F.sum("valor").alias("sum"),
#                 F.mean("valor").alias("mean"),
#             )
#         .withColumn(fabrica colunas dimensao)
#
# )

import pyspark.sql.functions as F

VISAO = {
    "client_id": "cliente",
    "estabelecimento": "estabelecimento",
    "client_id-estabelecimento": "cliente_estabelecimento",
}


def _get_vision_name(grouped_columns):
    return (
        VISAO.get("-".join(grouped_columns))
        if len(grouped_columns) > 1
        else VISAO.get(*grouped_columns)
    )


class DebitoAggregate:
    def create_aggregations(self, vision, df: DataFrame):
        pprint(vision)
        dimensions = vision.get("dimensions", {})
        df = UnionFrames.create(
            [
                df.filter(
                    reduce(and_, pf.dispatcher(combination)),
                )
                .groupBy(*vision.get("grouped_by"))
                .agg(*mf.create_expressions(vision.get("metrics", {})))
                .transform(
                    lambda _df: dimf.create_columns(
                        _df,
                        combination,
                        _get_vision_name(vision.get("grouped_by")),
                    )
                )
                for combination in [
                    dict(zip(dimensions.keys(), combination))
                    for combination in product(*list(dimensions.values()))
                ]
            ]
        )
        print(_get_vision_name(vision.get("grouped_by")))
        print(df.count())
        return df
