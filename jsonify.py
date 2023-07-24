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
print(f"{dx.columns=}")
print(f"{len(dx.columns)=}")
dx = dx.withColumn(
    "_pivot",
    F.concat_ws("-", "subproduto", "janela", "periodo"),
)
print(f"{dx.columns=}")
print(f"{len(dx.columns)=}")
# dx.show(truncate=False)
dx = (
    dx.groupBy("client_id")
    .pivot("_pivot")
    .agg(
        F.first("count").alias("count"),
        # F.first("soma").alias("soma"),
        # F.first("media").alias("media"),
        # F.first("maximo").alias("maximo"),
        # F.first("minimo").alias("minimo"),
        F.first("desviopadrao").alias("desviopadrao"),

        # F.first("mediana").alias("mediana"),

        # F.first("q1").alias("q1"),
        # F.first("q2").alias("q2"),
        # F.first("q3").alias("q3"),
        # F.first("q4").alias("q4"),
        #
        # F.first("d1").alias("d1"),
        # F.first("d2").alias("d2"),
        # F.first("d3").alias("d3"),
        # F.first("d4").alias("d4"),
        # F.first("d5").alias("d5"),
        # F.first("d6").alias("d6"),
        # F.first("d7").alias("d7"),
        # F.first("d8").alias("d8"),
        # F.first("d9").alias("d9"),
        # F.first("d10").alias("d10"),

    )
)
# dx.show(truncate=False)
print(f"{dx.columns=}")
print(f"{len(dx.columns)=}")
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
# pprint(rel_col)
# pprint(nested)
# dx.show(truncate=False)


dx = dx.withColumn("metricas", nested_to_json(nested))
dx.select("client_id", "metricas").show(truncate=False)
