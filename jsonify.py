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
        keys = key.split("#")
        last_key = keys.pop()
        reduce(lambda d, k: d.setdefault(k, {}), keys, result)[last_key] = value
        return result

    return reduce(add_to_nested_dict, input_dict.items(), {})


metric_renamer = {
    "desviopadrao": "desvio_padrao",
    "maximo": "max",
    "minimo": "min",
}


def nested_to_json(nested_dict):
    def convert_to_json(d):
        if isinstance(d, dict):
            return F.struct(
                *[
                    convert_to_json(d[dimension]).alias(
                        metric_renamer.get(dimension, dimension)
                    )
                    for dimension in d
                ]
            )
        return d

    return convert_to_json(nested_dict)


spark = SparkSession.builder.appName("Python Spark SQL basic example").getOrCreate()

dx = spark.read.format("parquet").load(
    "AGGREGATED/visao=cliente/data_processamento=2023-07-26"
)
CAMPOS_VISAO = ["client_id"]
DIMENSION_NAME = ["subproduto", "janela", "periodo"]
SPECIFI_METRICS = ['datediffwithmindate', 'datediffwithmaxdate']
# print(f"{dx.columns=}")
# print(f"{len(dx.columns)=}")
first_colunas = list(filter(lambda coluna: coluna not in {*DIMENSION_NAME, *CAMPOS_VISAO}, dx.columns))
print(f"{first_colunas=}")
# exit()
dx = dx.withColumn(
    "_pivot",
    F.concat_ws("#", *DIMENSION_NAME),
)
dx = dx.select(sorted(dx.columns))
dx.show(truncate=False)
# print(f"{dx.columns=}")
# print(f"{len(dx.columns)=}")
# dx.show(truncate=False)
dx = (
    dx.groupBy("client_id")
    .pivot("_pivot")
    .agg(
        *[F.first(metric, ignorenulls=(True if metric in SPECIFI_METRICS else False)).alias(metric) for metric in first_colunas]
        
        # F.first("maximoDataTransacao").alias('maximoDataTransacao'),
        # F.first("minimoDataTransacao").alias('minimoDataTransacao'),
        # F.first("maximoValor").alias('maximoValor'),
        # F.first("minimoValor").alias('minimoValor'),
        # F.first("cluster0to500Valor").alias('cluster0to500Valor'),
        # F.first("cluster500to1000Valor").alias('cluster500to1000Valor'),
        # F.first("cluster1000to2000Valor").alias('cluster1000to2000Valor'),
        # F.first("cluster2000to3000Valor").alias('cluster2000to3000Valor'),
        # F.first("cluster3000to5000Valor").alias('cluster3000to5000Valor'),
        # F.first("cluster5000to7000Valor").alias('cluster5000to7000Valor'),
        # F.first("cluster7000to10000Valor").alias('cluster7000to10000Valor'),
        # F.first("clusterBeyond10000Valor").alias('clusterBeyond10000Valor'),
        # F.first("somaValor").alias('somaValor'),
        # F.first("mediaValor").alias('mediaValor'),
        # F.first("countValor").alias('countValor'),
        # F.first("desviopadraoValor").alias('desviopadraoValor'),
        # F.first("medianaValor").alias('medianaValor'),
        # F.first("d1Valor").alias('d1Valor'),
        # F.first("d2Valor").alias('d2Valor'),
        # F.first("d3Valor").alias('d3Valor'),
        # F.first("d4Valor").alias('d4Valor'),
        # F.first("d5Valor").alias('d5Valor'),
        # F.first("d6Valor").alias('d6Valor'),
        # F.first("d7Valor").alias('d7Valor'),
        # F.first("d8Valor").alias('d8Valor'),
        # F.first("d9Valor").alias('d9Valor'),
        # F.first("d10Valor").alias('d10Valor'),
        # F.first("q1Valor").alias('q1Valor'),
        # F.first("q2Valor").alias('q2Valor'),
        # F.first("q3Valor").alias('q3Valor'),
        # F.first("q4Valor").alias('q4Valor'),


        # F.first("datediffwithmindate", ignorenulls=True).alias("datediffwithmindate"),
        # F.first("datediffwithmaxdate", ignorenulls=True).alias("datediffwithmaxdate"),
    )
)
dx.show(truncate=False)
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
rel_col = {
    "#".join(coluna.rsplit("_", 1)): F.col(coluna).alias(f"{coluna}")
    for coluna in colunas
}
nested = create_nested_dict(rel_col)

pprint(f"{nested=}")
# dx.show(truncate=False)
# exit()


dx = dx.withColumn("metricas", nested_to_json(nested)).select("client_id", "metricas")
dx.printSchema()
dx.show(truncate=False)
# dx.printSchema()
dx.write.format("json").mode("overwrite").save("aqui")
print(f"{dx.count()=}")
