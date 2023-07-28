from functools import reduce
import json
from data_writer import DataWriter
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
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType


def create_nested_dict(input_dict: Dict[str, Any]) -> Dict[str, Any]:
    def add_to_nested_dict(
        result: Dict[str, Any], key_value: Iterable[Tuple[str, Any]]
    ) -> Dict[str, Any]:
        key, value = key_value
        keys = key.split("#")
        last_key = keys.pop()
        reduce(lambda d, k: d.setdefault(k, {}), keys, result)[last_key] = value
        return result

    return reduce(add_to_nested_dict, input_dict.items(), {})


def nested_to_json(nested_dict):
    def convert_to_json(d):
        if isinstance(d, dict):
            return F.struct(
                *[convert_to_json(d[dimension]).alias(dimension) for dimension in d]
            )
        return (
            F.coalesce(d, F.lit(0))
            if not any(metric in d._jc.toString() for metric in SPECIFI_METRICS)
            else d
        )

    return convert_to_json(nested_dict)


spark = SparkSession.builder.appName("Python Spark SQL basic example").getOrCreate()
CAMPOS_VISAO = ["estabelecimento"]
DIMENSION_NAME = ["subproduto", "janela", "periodo"]
SPECIFI_METRICS = [
    "diasDesdePrimeiraTransacao",
    "diasDesdeUltimaTransacao",
    "countDistinctCliente",
    "countDistinctEstabelecimento",
]
relation = {
    "cliente": ("client_id",),
    "estabelecimento": ("estabelecimento",),
    "cliente_estabelecimento": ("client_id", "estabelecimento"),
}
for vision in relation:
    dx = (
        spark.read.format("json")
        .load(f"AGGREGATED/visao={vision}/")
        .filter(F.col("data_processamento") == "2023-07-27")
        .drop("data_processamento")
    )
    print(f"{vision=}")

    first_colunas = list(
        filter(
            lambda coluna: coluna not in {*DIMENSION_NAME, *relation[vision]},
            dx.columns,
        )
    )
    print(f"{first_colunas=}")

    dx = dx.withColumn(
        "_pivot",
        F.concat_ws("#", *DIMENSION_NAME),
    )

    dx = (
        dx.groupBy(*relation[vision])
        .pivot("_pivot")
        .agg(
            *[
                F.first(
                    metric, ignorenulls=(True if metric in SPECIFI_METRICS else False)
                ).alias(metric)
                for metric in first_colunas
            ]
        )
    )

    import itertools

    subproduto = [
        "debito_com_autenticacao",
        "debito_sem_autenticacao",
        "contactless_sem_autentificacao",
        "geral",
    ]

    janela = [
        "ultimos_7_dias",
        "ultimos_30_dias",
        "ultimos_90_dias",
        "ultimos_180_dias",
        "ultimos_270_dias",
        "ultimos_365_dias",
    ]

    periodo = ["diurno", "noturno", "geral"]

    metricas = ["countDistinctEstabelecimento"]


    def replace_last_hash(item):
        return "_".join(item.rsplit("#", 1))

    combined_list = list(map(replace_last_hash, [
        '#'.join(items)
        for items in itertools.product(subproduto, janela, periodo, metricas)
    ]))
    

    filtered_list = list(
        filter(
            lambda item: (
                item.endswith("countDistinctEstabelecimento")
                and item not in combined_list
            ),
            dx.columns,
        ),
    )

    print(f"{filtered_list=}")
    dx = dx.drop(*filtered_list)
    colunas = list(filter(lambda coluna: coluna not in {*relation[vision]}, dx.columns))

    rel_col = {
        "#".join(coluna.rsplit("_", 1)): F.col(coluna).alias(f"{coluna}")
        for coluna in colunas
    }
    nested = create_nested_dict(rel_col)

    dx = (
        dx.withColumn("metricas", nested_to_json(nested))
        .select(*relation[vision], "metricas")
        .withColumn("visao", F.lit(vision))
    )

    DataWriter.save(
        dx,
        writing_details={
            "partitions": ["visao"],
            "saving_path": "aqui4",
            "saving_format": "json",
            "saving_mode": "overwrite",
        },
    )
    dx.show()

    print(f"{dx.schema.json()=}")

    print(f"{dx.count()=}")
