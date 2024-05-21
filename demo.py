from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Row
from random import choice
from itertools import product
spark = SparkSession.builder.appName("Safe").getOrCreate()


schema = [
    "nome_visao",
    "valor_visao",
    "janela",
    "nome_modelo",
    "quantidadeDocumentosDistintos",
    "somaDocumentosDistintos",
    "data_processamento",
]
colunas = [
    "karina_ultimos12Dias_quantidadeDocumentosDistintos",
    "karina_ultimos12Dias_somaDocumentosDistintos",
    "karina_ultimos3Dias_quantidadeDocumentosDistintos",
    "karina_ultimos3Dias_somaDocumentosDistintos",
    "karina_ultimos9Dias_quantidadeDocumentosDistintos",
    "karina_ultimos9Dias_somaDocumentosDistintos",
    "scuptor_ultimos12Dias_quantidadeDocumentosDistintos",
    "scuptor_ultimos12Dias_somaDocumentosDistintos",
    "scuptor_ultimos3Dias_quantidadeDocumentosDistintos",
    "scuptor_ultimos3Dias_somaDocumentosDistintos",
    "scuptor_ultimos9Dias_quantidadeDocumentosDistintos",
    "scuptor_ultimos9Dias_somaDocumentosDistintos",
]

nomes_modelo = ["karina", "scuptor"]
janelas = ["ultimos3Dias", "ultimos9Dias", "ultimos12Dias"]
metricas = ["quantidadeDocumentosDistintos", "somaDocumentosDistintos"]
combinations = product(nomes_modelo, janelas, metricas)
result = [
    "{}_{}_{}".format(nome, janela, metrica) for nome, janela, metrica in combinations
]

rows = [
    Row(
        nome_visao=choice(["origem", "destino"]),
        valor_visao=f"name_{choice(list(range(3)))}",
        janela=choice(["ultimos3Dias", "ultimos9Dias", "ultimos12Dias"]),
        nome_modelo=choice(["karina", "scuptor"]),
        quantidadeDocumentosDistintos=choice([*list(range(10)), None]),
        somaDocumentosDistintos=choice([*list(range(10)), None]),
        data_processamento=choice(["2020-01-01", "2024-04-04"]),
    )
    for i in range(30)
]

df = spark.createDataFrame(rows, schema=schema)
print(df.count())
df.show(50)
metric_columns = ["quantidadeDocumentosDistintos", "somaDocumentosDistintos"]
group_by_columns = ["nome_visao", "valor_visao", "data_processamento"]
pivot_columns = ["nome_modelo", "janela"]
df = df
df = (
    df.withColumn(
        "pivot", F.concat_ws("_", *[F.col(pivot_me) for pivot_me in pivot_columns])
    )
    .groupBy(*group_by_columns)
    .pivot("pivot")
    .agg(
        *[F.first(metric, ignorenulls=True).alias(metric) for metric in metric_columns]
    )
    # .fillna(default_values)
)
print(df.columns)
df.show()
print(df.count())
print(f"{len(df.columns)=}")
partition_dates = (
    df.select("data_processamento")
    .distinct()
    .select(F.collect_list("data_processamento"))
    .first()[0]
)
print(f"{partition_dates=}")

assert sorted(colunas) == sorted(result)
