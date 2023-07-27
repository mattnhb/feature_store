from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

from functools import reduce
from pprint import pprint
from operator import and_
from pyspark.sql import DataFrame
from itertools import product
import pyspark.sql.functions as F
from dimensions import DimensionsFactory
from metrics import MetricsFactory
from predicates import PredicateFactory
from utils import get_vision_name, union_frames


class BaseAggregator(ABC):
    def __init__(self) -> None:
        self.pf = PredicateFactory()
        self.dimf = DimensionsFactory()
        self.mf = MetricsFactory()
        super().__init__()

    def create_general_unified_aggregations(
        self, df: DataFrame, grouped_by, dimensions, metrics
    ) -> DataFrame:
        return union_frames(
            [
                df.filter(
                    reduce(and_, self.pf.dispatcher(combination)),
                )
                .groupBy(*grouped_by)
                .agg(*self.mf.create_expressions(metrics))
                .transform(lambda _df: self.dimf.create_columns(_df, combination))
                for combination in [
                    dict(zip(dimensions.keys(), combination))
                    for combination in product(*list(dimensions.values()))
                ]
            ]
        )

    @abstractmethod
    def create_specific_unified_aggregations(
        self, df: DataFrame, *args, **kwargs
    ) -> DataFrame:
        pass
