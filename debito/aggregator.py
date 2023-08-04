from functools import reduce
from pprint import pprint
from operator import and_
from typing import List
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


class DebitoAggregator(BaseAggregator):
    def __init__(self, vision) -> None:
        self.vision_relation = {
            ("client_id",): "cliente",
            ("estabelecimento",): "estabelecimento",
            ("client_id", "estabelecimento"): "cliente_estabelecimento",
        }
        self.vision = vision
        self.vision_name = get_vision_name(
            vision.get("grouped_by"), vision_relation=self.vision_relation
        )
        super().__init__()

    def create_aggregations(self, df: DataFrame):
        # print(f"{df.rdd.getNumPartitions()=}")
        # print(f"total linhas nos snapshots -> {df.count()}")
        # df.count()
        aggregated: DataFrame = self.create_general_unified_aggregations(
            df,
            grouped_by=self.vision.get("grouped_by"),
            dimensions=self.vision.get("general", {}).get("dimensions", {}),
            metrics=self.vision.get("general").get("metrics", {}),
        )
        exit()
        if specific := self.create_specific_unified_aggregations(
            df, enabled=self.vision.get("specific", False)
        ):
            aggregated = union_frames([aggregated, *specific])

        return aggregated.withColumn(
            "visao",
            F.lit(self.vision_name),
        )  # .cache().persist()#.distinct()

    def _count_field_distinct(self, df: DataFrame) -> List[DataFrame]:
        if self.vision_name == "cliente_estabelecimento":
            return []
        # colocar namedtuple se encaixar legal
        target = (
            ("client_id", "cliente")
            if self.vision_name == "estabelecimento"
            else ("estabelecimento", "estabelecimento")
        )
        print(f"{target=}")
        _dim_combinations = {
            "cliente": {
                "subproduto": [
                    {"debito_com_autenticacao": "subproduto"},
                    {"debito_sem_autenticacao": "subproduto"},
                    {"contactless_sem_autentificacao": "subproduto"},
                    {"geral": "subproduto"},
                ],
                "janela": [
                    {"ultimos_7_dias": "data_transacao"},
                    {"ultimos_30_dias": "data_transacao"},
                    {"ultimos_90_dias": "data_transacao"},
                    {"ultimos_180_dias": "data_transacao"},
                    {"ultimos_270_dias": "data_transacao"},
                    {"ultimos_365_dias": "data_transacao"},
                ],
                "periodo": [
                    {"diurno": "data_transacao"},
                    {"noturno": "data_transacao"},
                    {"geral": "data_transacao"},
                ],
            },
            "estabelecimento": {
                "subproduto": [
                    {"debito_com_autenticacao": "subproduto"},
                    {"debito_sem_autenticacao": "subproduto"},
                    {"contactless_sem_autentificacao": "subproduto"},
                    {"geral": "subproduto"},
                ],
                "janela": [
                    {"ultimos_7_dias": "data_transacao"},
                    {"ultimos_30_dias": "data_transacao"},
                    {"ultimos_90_dias": "data_transacao"},
                    {"ultimos_180_dias": "data_transacao"},
                    {"ultimos_270_dias": "data_transacao"},
                    {"ultimos_365_dias": "data_transacao"},
                ],
                "periodo": [
                    {"geral": "data_transacao"},
                ],
            },
        }
        return [
            self.create_general_unified_aggregations(
                df,
                grouped_by=self.vision.get("grouped_by"),
                dimensions=_dim_combinations[self.vision_name],
                metrics=[
                    {"count_distinct": [target[0], f"countDistinct{target[1].title()}"]}
                ],
            )
        ]

    def _date_diff_today_first_last(self, df: DataFrame) -> List[DataFrame]:
        min_date = F.min(F.col("data_transacao")).over(
            Window.partitionBy("client_id").orderBy(F.col("data_transacao"))
        )
        max_date = F.min(F.col("data_transacao")).over(
            Window.partitionBy("client_id").orderBy(F.col("data_transacao").desc())
        )
        combinations = (
            {"periodo": "geral", "subproduto": "geral", "condition": F.lit(True)},
        )
        return [
            (
                df.where(combination["condition"])
                .withColumn(
                    "diasDesdePrimeiraTransacao",
                    F.datediff(F.current_date(), min_date),
                )
                .withColumn(
                    "diasDesdeUltimaTransacao",
                    F.datediff(F.current_date(), max_date),
                )
                .groupBy(*self.vision.get("grouped_by"))
                .agg(
                    F.first(F.col("diasDesdePrimeiraTransacao")).alias(
                        "diasDesdePrimeiraTransacao"
                    ),
                    F.first(F.col("diasDesdeUltimaTransacao")).alias(
                        "diasDesdeUltimaTransacao"
                    ),
                )
                .withColumn("janela", F.lit("ultimos_365_dias"))
                .withColumn("periodo", F.lit(combination["periodo"]))
                .withColumn("subproduto", F.lit(combination["subproduto"]))
            )
            for combination in combinations
        ]

    def create_specific_unified_aggregations(
        self, df: DataFrame, enabled: bool
    ) -> DataFrame:
        if enabled:
            x = [
                *self._date_diff_today_first_last(df),
                #  *self._count_field_distinct(df)
            ]

            return x
