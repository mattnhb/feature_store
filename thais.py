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


spark = SparkSession.builder.appName("Python Spark SQL basic example").getOrCreate()

df = spark.createDataFrame(
    [
        Row(
            id_transacao=fake.uuid4(),
            optype=choice(["abertura_tapete_laranja", "pix"]),
            data_hora=fake.date_time_between(start_date="-10d"),
            valor=choice(range(50)),
        )
        for _ in range(10000)
    ]
)
df.show(truncate=False)
df = df.withColumn("dia_semana", F.dayofweek("data_hora"))
df.show(truncate=False)
# w = Window().partitionBy("optype").orderBy("valor")
# df.withColumn("row_number", F.row_number().over(w)).show(truncate=False)
d0 = (
    df.withColumn("now", F.current_date())
    .where(F.col("optype") == F.lit("pix"))
    .groupBy("optype")
    .agg(
        F.count("valor").alias("count"),
        F.sum("valor").alias("sum"),
        F.mean("valor").alias("mean"),
    )
    .withColumn("janela", F.lit("foda"))
    .withColumn("periodo", F.lit("geral"))
).withColumn("subproduto", F.lit("debito contactless")).withColumn("firula", F.lit("14"))

d1 = (
    df.withColumn("now", F.current_date())
    .where(F.datediff("now", "data_hora") <= F.lit(5))
    .groupBy("optype")
    .agg(
        F.count("valor").alias("count"),
        F.sum("valor").alias("sum"),
        F.mean("valor").alias("mean"),
    )
    .withColumn("janela", F.lit("5_dias"))
    .withColumn("periodo", F.lit("geral"))
).withColumn("subproduto", F.lit("debito sem aut")).withColumn("firula", F.lit("31"))
d2 = (
    df.withColumn("now", F.current_date())
    .where(F.datediff("now", "data_hora") <= F.lit(3))
    .groupBy("optype")
    .agg(
        F.count("valor").alias("count"),
        F.sum("valor").alias("sum"),
        F.mean("valor").alias("mean"),
    )
    .withColumn("janela", F.lit("3_dias"))
    .withColumn("periodo", F.lit("geral"))
).withColumn("subproduto", F.lit("debito com aut")).withColumn("firula", F.lit("1"))
d3 = (
    df.withColumn("now", F.current_date())
    .where(
        (F.datediff("now", "data_hora") <= F.lit(3))
        & (F.col("dia_semana").isin([1, 7]))
    )
    .groupBy("optype")
    .agg(
        F.count("valor").alias("count"),
        F.sum("valor").alias("sum"),
        F.mean("valor").alias("mean"),
    )
    .withColumn("janela", F.lit("5_dias"))
    .withColumn("periodo", F.lit("fdsemana"))
).withColumn("subproduto", F.lit("debito com aut")).withColumn("firula", F.lit("2"))
d4 = (
    df.withColumn("now", F.current_date())
    .where(
        (F.datediff("now", "data_hora") <= F.lit(5))
        & (F.col("dia_semana").isin([1, 7]))
    )
    .groupBy("optype")
    .agg(
        F.count("valor").alias("count"),
        F.sum("valor").alias("sum"),
        F.mean("valor").alias("mean"),
    )
    .withColumn("janela", F.lit("5_dias"))
    .withColumn("periodo", F.lit("fdsemana"))
).withColumn("subproduto", F.lit("debito com aut")).withColumn("firula", F.lit("1")).withColumn("teste", F.lit("bala"))

dx = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), {d0, d1, d2, d3, d4}).distinct()
# dx = d1.union(d2).union(d3).union(d0).unionByName(d4, allowMissingColumns=True)
dx.show(truncate=False)

dx = dx.withColumn("_pivot", F.concat_ws("-", "subproduto", "janela", "periodo", "firula", "teste"))
dx.show(truncate=False)

dx = (
    dx.groupBy("optype")
    .pivot("_pivot")
    .agg(
        F.first("count").alias("count"),
        F.first("sum").alias("sum"),
        F.first("mean").alias("mean"),
    )
)
dx.show(truncate=False)
print(dx.columns)
colunas = list(filter(lambda coluna: coluna not in {"optype"}, dx.columns))
print(f"{colunas=}")

# exit()
from typing import Dict, Any, Iterable, Tuple


def create_nested_dict(input_dict: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a nested dictionary from a flat dictionary with keys separated by "-".

    Args:
        input_dict (Dict[str, Any]): The input dictionary with keys separated by "-".

    Returns:
        Dict[str, Any]: The nested dictionary with keys split at the "-" character.
    """
    def add_to_nested_dict(result: Dict[str, Any], key_value: Iterable[Tuple[str, Any]]) -> Dict[str, Any]:
        key, value = key_value
        keys = key.split("-")
        last_key = keys.pop()
        reduce(lambda d, k: d.setdefault(k, {}), keys, result)[last_key] = value
        return result

    return reduce(add_to_nested_dict, input_dict.items(), {})

dx = reduce(lambda df, coluna: df.withColumnRenamed(coluna, "-".join(coluna.rsplit("_", 1))), colunas, dx)
# new_colunas = ["-".join(coluna.rsplit("_", 1)) for coluna in colunas]

rel_col = {coluna: F.col(coluna) for coluna in ["-".join(coluna.rsplit("_", 1)) for coluna in colunas]}
# print(f"{new_colunas=}")
nested = create_nested_dict(rel_col)
pprint(rel_col)
pprint(nested)
dx.show(truncate=False)
# exit()



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


dx = dx.withColumn(
    "metricas",
    nested_to_json(nested)
)
dx.select("optype", "metricas").show(truncate=False)
# df.agg(F.collect_list("optype"), F.collect_list("valor")).show(truncate=False)
# w = df.groupBy(F.window("data_hora", "24 hours 3 minutes"), "optype").agg(F.count("data_hora").alias("count"), F.sum("valor").alias("sum"), F.mean("valor").alias("mean"))
# df = w.select(w.window.start.cast("string").alias("start"), w.window.end.cast("string").alias("end"), "count", "sum", "mean", "optype").orderBy('count', ascending=False)
# df.show(truncate=False)
# df.groupBy("optype").agg(F.max("count").alias("maximo de tpm"), F.mean("count").alias("tpm medio")).show(truncate=False)
