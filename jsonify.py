from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql import Row
from faker import Faker
from random import choice
from pyspark.sql.window import Window
from pprint import pprint

fake = Faker()
Faker.seed(0)

from pyspark.sql import SparkSession
from typing import Dict, Any, Iterable, Tuple


def create_nested_dict(input_dict: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a nested dictionary from a flat dictionary with keys separated by "-".

    Args:
        input_dict (Dict[str, Any]): The input dictionary with keys separated by "-".

    Returns:
        Dict[str, Any]: The nested dictionary with keys split at the "-" character.
    """

    def add_to_nested_dict(
        result: Dict[str, Any], key_value: Iterable[Tuple[str, Any]]
    ) -> Dict[str, Any]:
        key, value = key_value
        keys = key.split("-")
        last_key = keys.pop()
        reduce(lambda d, k: d.setdefault(k, {}), keys, result)[
            last_key
        ] = value
        return result

    return reduce(add_to_nested_dict, input_dict.items(), {})


def nested_to_json(nested_dict):
    def convert_to_json(d):
        if isinstance(d, dict):
            return F.struct(
                *[
                    convert_to_json(d[dimension]).alias(dimension)
                    for dimension in d
                ]
            )
        return d

    return F.to_json(convert_to_json(nested_dict))


spark = SparkSession.builder.appName(
    "Python Spark SQL basic example"
).getOrCreate()

dx = spark.read.format("parquet").load("AGGREGATED")

dx = dx.withColumn(
    "_pivot",
    F.concat_ws("-", "subproduto", "janela", "periodo"),
)
dx.show(truncate=False)
dx = (
    dx.groupBy("client_id")
    .pivot("_pivot")
    .agg(
        F.first("count").alias("count"),
        F.first("soma").alias("soma"),
        F.first("media").alias("media"),
    )
)
dx.show(truncate=False)
colunas = list(filter(lambda coluna: coluna not in {"client_id"}, dx.columns))
dx = reduce(
    lambda df, coluna: df.withColumnRenamed(
        coluna, "-".join(coluna.rsplit("_", 1))
    ),
    colunas,
    dx,
)
rel_col = {
    coluna: F.col(coluna)
    for coluna in ["-".join(coluna.rsplit("_", 1)) for coluna in colunas]
}
nested = create_nested_dict(rel_col)
pprint(rel_col)
pprint(nested)
dx.show(truncate=False)


dx = dx.withColumn("metricas", nested_to_json(nested))
dx.select("client_id", "metricas").show(truncate=False)
