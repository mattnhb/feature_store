from datetime import timedelta, timezone

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

dt_proc = "2023-06-04"
default_relation = {
    "quantia": 0,
}
# default_relation.update({dt_proc: "1900-01-01"})
def nested_to_json(nested_dict):
    def convert_to_json(d):
        if isinstance(d, dict):
            return F.struct(
                *[convert_to_json(d[dimension]).alias(dimension) for dimension in d]
            )
        print(f"{default_relation=}")
        print(f"{d._jc.toString()=}")
        # return d
        return F.coalesce(d, F.lit(default_relation.get(d._jc.toString(), 0)))

    return convert_to_json(nested_dict)


spark = SparkSession.builder.appName("Fake Data").getOrCreate()
relation1 = {"ddd": F.lit("11"), "numero": F.lit("54554")}
relation2 = {"ddd": F.lit("21"), "numero": F.lit("111212")}
relation3 = {"ddd": F.lit("31"), "numero": F.lit("9898")}
canais = ("mobile", "internetBanking")
df = spark.createDataFrame(
    [
        Row(
            index=n,
            quantia=n * 1,
            soma=n * 2,
            media=n * 3,
            canal=canais[n - 1],
        )
        for n in range(1, 3)
    ]
)
# df = df.withColumn("telefones", F.array(nested_to_json(relation1), nested_to_json(relation2), nested_to_json(relation3)))
df.show(truncate=False)
# df.printSchema()
# df.withColumn("telefone_ddd", F.expr("transform(telefones,p -> p.ddd)")).withColumn("telefone_numero", F.expr("transform(telefones,p -> p.numero)")).show(truncate=False)
from functools import reduce

# df = df.withColumn("_pivot", F.concat_ws("#", ))
df = (
    df.groupBy("index")
    .pivot("canal")
    .agg(
        F.first("quantia", ignorenulls=True).alias("quantia"),
        F.first("soma", ignorenulls=True).alias("soma"),
        F.first("media", ignorenulls=True).alias("media"),
    )
)
df.show(truncate=False)
relation = {
    "internetBanking": {
        "quantia": F.col("internetBanking_quantia"),
        "soma": F.col("internetBanking_soma"),
        "media": F.col("internetBanking_media"),
    },
    "mobile": {
        "quantia": F.col("mobile_quantia"),
        "soma": F.col("mobile_soma"),
        "media": F.col("mobile_media"),
    },
    "data_processamento": F.lit("saas")
}
df = df.withColumn(
        "metricas",
        nested_to_json(relation),
    ).select("index", "metricas")
df.show(truncate=False)
df.printSchema()
# def create_col(df, relations):
#     return reduce(lambda acc_df, column: acc_df.withColumn(column[1], F.lit(column[0])), reduce(lambda x, y: {**x, **y}, relations).items(), df)
# relations = [{"safe": "1"}, {"turbo": "2"}]

# x = reduce(lambda x, y: {**x, **y}, relations)
# df.transform(lambda _df: create_col(_df, relations)).show()
# print(f"{x=}")
# dx = (
#         dx.groupBy(*handler.grouped_by)
#         .pivot("_pivot")
#         .agg(
#             *[
#                 F.first(
#                     metric,
#                     ignorenulls=(True if metric in handler.specific_metrics else False),
#                 ).alias(metric)
#                 for metric in first_colunas
#             ]
