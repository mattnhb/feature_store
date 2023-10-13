from datetime import timedelta, timezone

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def nested_to_json(nested_dict):
    def convert_to_json(d):
        if isinstance(d, dict):
            return F.struct(
                *[convert_to_json(d[dimension]).alias(dimension) for dimension in d]
            )
        return d

    return convert_to_json(nested_dict)


spark = SparkSession.builder.appName("Fake Data").getOrCreate()
relation1 = {"ddd": F.lit("11"), "numero": F.lit("54554")}
relation2 = {"ddd": F.lit("21"), "numero": F.lit("111212")}
relation3 = {"ddd": F.lit("31"), "numero": F.lit("9898")}
df = spark.createDataFrame([Row(index=n) for n in range(400)])
df = df.withColumn("telefones", F.array(nested_to_json(relation1), nested_to_json(relation2), nested_to_json(relation3)))
df.show(truncate=False)
df.printSchema()
df.withColumn("telefone_ddd", F.expr("transform(telefones,p -> p.ddd)")).withColumn("telefone_numero", F.expr("transform(telefones,p -> p.numero)")).show(truncate=False)
