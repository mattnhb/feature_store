from pprint import pprint

import pyspark.sql.functions as F
from operator import and_


# 'metrics': [{'soma': 'valor'}, {'media': 'valor'}, {'maximo': 'valor'}]}

# metrics=[{'soma': 'valor'}, {'media': 'valor'}, {'maximo': 'valor'}]


class MetricsFactory:
    def create_expressions(self, metrics):
        _metrics = {
            "soma": F.sum,
            "media": F.mean,
            "maximo": F.max,
            "count": F.count,
        }
        print(f"{metrics=}")
        for metric in metrics:
            print(metric)
        return [
            _metrics.get(metric_name)(column).alias(metric_name)
            for metric in metrics
            for metric_name, column in metric.items()
        ]
