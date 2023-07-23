from functools import reduce
from pprint import pprint
from operator import and_
from pyspark.sql import DataFrame
from itertools import product

from dimensions import DimensionsFactory
from metrics import MetricsFactory
from predicates import PredicateFactory

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
#         .withColumn(fabrica colunas dimensao)
#
# )


class DebitoAggregate:
    def create_aggregations(self, vision, df: DataFrame):
        pprint(vision)
        dimensions = vision.get("dimensions", {})
        combinations = [
            dict(zip(dimensions.keys(), combination))
            for combination in product(*list(dimensions.values()))
        ]
        for combination in combinations:
            df.filter(
                reduce(and_, pf.dispatcher(combination)),
            ).show(truncate=False)
        return df
