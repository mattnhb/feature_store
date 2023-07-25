from pprint import pprint

import pyspark.sql.functions as F
from operator import and_


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
            "d1": lambda coluna: F.expr(f"percentile({coluna}, 0.1)"),
            "d2": lambda coluna: F.expr(f"percentile({coluna}, 0.2)"),
            "d3": lambda coluna: F.expr(f"percentile({coluna}, 0.3)"),
            "d4": lambda coluna: F.expr(f"percentile({coluna}, 0.4)"),
            "d5": lambda coluna: F.expr(f"percentile({coluna}, 0.5)"),
            "d6": lambda coluna: F.expr(f"percentile({coluna}, 0.6)"),
            "d7": lambda coluna: F.expr(f"percentile({coluna}, 0.7)"),
            "d8": lambda coluna: F.expr(f"percentile({coluna}, 0.8)"),
            "d9": lambda coluna: F.expr(f"percentile({coluna}, 0.9)"),
            "d10": lambda coluna: F.expr(f"percentile({coluna}, 1.0)"),
            "q1": lambda coluna: F.expr(f"percentile({coluna}, 0.25)"),
            "q2": lambda coluna: F.expr(f"percentile({coluna}, 0.5)"),
            "q3": lambda coluna: F.expr(f"percentile({coluna}, 0.75)"),
            "q4": lambda coluna: F.expr(f"percentile({coluna}, 1.0)"),
        }
        return [
            _metrics.get(metric_name)(column).alias(metric_name)
            for metric in metrics
            for metric_name, column in metric.items()
        ]
