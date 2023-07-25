from functools import reduce
from pprint import pprint
from operator import and_
from base.aggregator import BaseAggregator
from pyspark.sql import DataFrame
from itertools import product
import pyspark.sql.functions as F
from dimensions import DimensionsFactory
from metrics import MetricsFactory
from predicates import PredicateFactory
from union_frames import UnionFrames
from utils import union_frames

pf = PredicateFactory()
dimf = DimensionsFactory()
mf = MetricsFactory()


class DebitoAggregator(BaseAggregator):
    def create_aggregations(self, vision, df: DataFrame):
        general_dimensions = vision.get("general", {}).get("dimensions", {})
        general = df
        general: DataFrame = self.create_general_unified_aggregations(
            df,
            grouped_by=vision.get("grouped_by"),
            dimensions=vision.get("general", {}).get("dimensions", {}),
            metrics=vision.get("general").get("metrics", {}),
        )
        if specific := self.create_specific_unified_aggregations(
            df,
            grouped_by=vision.get("grouped_by"),
            dimensions=vision.get("specific", {}).get("dimensions", {}),
            metrics=vision.get("specific").get("metrics", {}),
        ):
            return union_frames([general, specific])
        return general

    def create_specific_unified_aggregations(
        self, df: DataFrame, grouped_by, dimensions, metrics
    ) -> DataFrame:
        print(f"{grouped_by=}", f"{dimensions=}", f"{metrics=}")
        return None
