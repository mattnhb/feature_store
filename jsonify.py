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

metric_renamer = {
    "desviopadrao": "desvio_padrao",
    "maximo": "max",
    "minimo": "min"

}
def nested_to_json(nested_dict):
    def convert_to_json(d):
        if isinstance(d, dict):
            return F.struct(
                *[
                    convert_to_json(d[dimension]).alias(metric_renamer.get(dimension, dimension))
                    for dimension in d
                ]
            )
        return d

    return (convert_to_json(nested_dict))


spark = SparkSession.builder.appName(
    "Python Spark SQL basic example"
).getOrCreate()

dx = spark.read.format("parquet").load("AGGREGATED")
# print(f"{dx.columns=}")
# print(f"{len(dx.columns)=}")
dx = dx.withColumn(
    "_pivot",
    F.concat_ws("-", "subproduto", "janela", "periodo"),
)
# print(f"{dx.columns=}")
# print(f"{len(dx.columns)=}")
# dx.show(truncate=False)
dx = (
    dx.groupBy("client_id")
    .pivot("_pivot")
    .agg(
        F.first("count").alias("count"),
        F.first("soma").alias("soma"),
        F.first("media").alias("media"),
        F.first("maximo").alias("maximo"),
        F.first("minimo").alias("minimo"),
        F.first("desviopadrao").alias("desviopadrao"),
        F.first("mediana").alias("mediana"),
        F.first("q1").alias("q1"),
        F.first("q2").alias("q2"),
        F.first("q3").alias("q3"),
        F.first("q4").alias("q4"),
        #
        F.first("d1").alias("d1"),
        F.first("d2").alias("d2"),
        F.first("d3").alias("d3"),
        F.first("d4").alias("d4"),
        F.first("d5").alias("d5"),
        F.first("d6").alias("d6"),
        F.first("d7").alias("d7"),
        F.first("d8").alias("d8"),
        F.first("d9").alias("d9"),
        F.first("d10").alias("d10"),
    )
)
# dx.show(truncate=False)
# print(f"{dx.columns=}")
# print(f"aqui_len {len(dx.columns)=}")
colunas = list(filter(lambda coluna: coluna not in {"client_id"}, dx.columns))
# dx = reduce(
#     lambda df, coluna: df.withColumnRenamed(coluna, "-".join(coluna.rsplit("_", 1))),
#     colunas,
#     dx,
# )
# print(f"{colunas=}")
# rel_col = {
#     _coluna: F.col(coluna)
#     for _coluna in ["-".join(coluna.rsplit("_", 1)) for coluna in colunas]
# }
# rel_col = {}
# for coluna in colunas:
#     x = coluna
#     y = "-".join(coluna.rsplit("_", 1))
#     rel_col.update({y: F.col(x)})
# print(f"{rel_col=}")
# exit()
# contactless_sem_autentificacao-ultimos_180_dias-dia_17_24_count
rel_col = { "-".join(coluna.rsplit("_", 1)): F.col(coluna).alias(f"{coluna}op") for coluna in colunas }
nested = create_nested_dict(rel_col)

# pprint(f"{nested=}")
# dx.show(truncate=False)
# exit()


dx = dx.withColumn("metricas", nested_to_json(nested)).select("client_id", "metricas")
dx.printSchema()
# dx.show(truncate=False)
# dx.printSchema()
dx.write.format("json").mode("overwrite").save("aqui")
