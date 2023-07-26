from functools import reduce
from pprint import pprint
from operator import and_
from base.aggregator import BaseAggregator
from pyspark.sql import DataFrame
from itertools import product
import pyspark.sql.functions as F
from pyspark.sql import Window
from dimensions import DimensionsFactory
from metrics import MetricsFactory
from predicates import PredicateFactory
from union_frames import UnionFrames
from utils import get_vision_name, union_frames

pf = PredicateFactory()
dimf = DimensionsFactory()
mf = MetricsFactory()


class DebitoAggregator(BaseAggregator):
    def create_aggregations(self, vision, df: DataFrame):
        aggregated: DataFrame = self.create_general_unified_aggregations(
            df,
            grouped_by=vision.get("grouped_by"),
            dimensions=vision.get("general", {}).get("dimensions", {}),
            metrics=vision.get("general").get("metrics", {}),
        )
        if specific := self.create_specific_unified_aggregations(
            df, enabled=vision.get("specific", False)
        ):
            aggregated = union_frames([aggregated, specific])
        return aggregated.withColumn(
            "visao",
            F.lit(
                get_vision_name(
                    vision.get("grouped_by"), vision_relation=self.vision_relation
                )
            ),
        )

    def create_specific_unified_aggregations(
        self, df: DataFrame, enabled: bool
    ) -> DataFrame:
        if enabled:
            min_date = F.min(F.col("data_transacao")).over(
                Window.partitionBy("client_id").orderBy(F.col("data_transacao"))
            )
            max_date = F.min(F.col("data_transacao")).over(
                Window.partitionBy("client_id").orderBy(F.col("data_transacao").desc())
            )
            combinations = (
                {"periodo": "geral", "subproduto": "geral", "condition": F.lit(True)},
                {
                    "periodo": "noturno",
                    "subproduto": "geral",
                    "condition": (F.hour("data_transacao") >= 22)
                    | (F.hour("data_transacao") < 6),
                },
                {
                    "periodo": "diurno",
                    "subproduto": "geral",
                    "condition": ~(F.hour("data_transacao") >= 22)
                    | (F.hour("data_transacao") < 6),
                },
            )
            return union_frames(
                [
                    (
                        df.where(combination["condition"])
                        .withColumn(
                            "datediffwithmindate",
                            F.datediff(F.current_date(), min_date),
                        )
                        .withColumn(
                            "datediffwithmaxdate",
                            F.datediff(F.current_date(), max_date),
                        )
                        .groupBy("client_id")
                        .agg(
                            F.first(F.col("datediffwithmindate")).alias(
                                "datediffwithmindate"
                            ),
                            F.first(F.col("datediffwithmaxdate")).alias(
                                "datediffwithmaxdate"
                            ),
                        )
                        .withColumn("janela", F.lit("ultimos_365_dias"))
                        .withColumn("periodo", F.lit(combination["periodo"]))
                        .withColumn("subproduto", F.lit(combination["subproduto"]))
                    )
                    for combination in combinations
                ]
            )
