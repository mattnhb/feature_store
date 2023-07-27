from pprint import pprint
import re
from typing import Dict
import pyspark.sql.functions as F
from operator import and_

_percentages: Dict[str, float] = {
    "d1": 0.1,
    "d2": 0.2,
    "d3": 0.3,
    "d4": 0.4,
    "d5": 0.5,
    "d6": 0.6,
    "d7": 0.7,
    "d8": 0.8,
    "d9": 0.9,
    "d10": 1.0,
    "q1": 0.25,
    "q2": 0.5,
    "q3": 0.75,
    "q4": 1.0,
}


def _is_percentile(metric_name):
    return re.match(r"^(d[1-9]|d10|q[1-4])$", metric_name)


def _is_cluster_between(metric_name):
    return re.match(r"^cluster-(\d+)-(\d+)$", metric_name)


def _is_cluster_beyond(metric_name):
    return re.match(r"^cluster-beyond-(\d+)$", metric_name)


class MetricsFactory:
    def create_expressions(self, metrics):
        _metrics = {
            "soma": lambda coluna: F.sum(coluna),
            "media": lambda coluna: F.mean(coluna),
            "maximo": lambda coluna: F.max(coluna),
            "minimo": lambda coluna: F.min(coluna),
            "desviopadrao": lambda coluna: F.stddev(coluna),
            "mediana": lambda coluna: F.expr(f"percentile({coluna}, 0.5)"),
            "count": lambda coluna: F.count(coluna),
            "count_distinct": lambda coluna: F.count_distinct(coluna),
            # "amount-days-min-max": lambda coluna:
        }

        def dispatcher(metric_name, column):
            if _is_percentile(metric_name):
                return lambda coluna: F.expr(
                    f"percentile({coluna}, {_percentages[metric_name]})"
                )
            elif match := _is_cluster_between(metric_name):
                return lambda coluna: F.sum(
                    F.when(
                        F.col(coluna).between(int(match.group(1)), int(match.group(2))),
                        1,
                    ).otherwise(0)
                )
            elif match := _is_cluster_beyond(metric_name):
                return lambda coluna: F.sum(
                    F.when(F.col(coluna) > F.lit(int(match.group(1))), 1).otherwise(0)
                )
            return _metrics.get(metric_name)

        return [
            dispatcher(metric_name, column)(column[0]).alias(column[1])
            for metric in metrics
            for metric_name, column in metric.items()
        ]
