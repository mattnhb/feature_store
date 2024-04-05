from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

spark = SparkSession.builder.appName("Safe").getOrCreate()

nested_schema = StructType([
    StructField("field1", StringType(), True),
    StructField("field2", IntegerType(), True),
    StructField("field3", ArrayType(StringType()), True)
])

main_schema = StructType([
    StructField("body", nested_schema, True)
])
df = spark.read.json("justa.json")

# df.show(truncate=False)
# df.printSchema()
replace_me = {
    r"^\[|\]$": "",
    r"None": "null",
    r"True": "true",
    r"False": "false",
    r"\{'": "open_bracket_quote",
    r"'\{": "quote_open_bracket",
    r"'\}": "quote_close_bracket",
    r"body'": "aspa_body",
    r"'": "",
    r"open_bracket_quote": r"\{'",
    r"quote_close_bracket": r"'\}",
    r"aspa_body": r"body'",
    r"quote_open_bracket": r"'\{",
}
for pattern, replacement in replace_me.items():
    df = df.withColumn("data", F.regexp_replace(F.col("data"), pattern, replacement))
df.show(truncate=False)
df.printSchema()
# exit()


rdd = df.select(F.col("data").alias("jsoncol")).rdd.map(lambda x: x.jsoncol)
newschema = spark.read.json(rdd).schema
df = df.select("*", F.from_json("data", newschema).alias("test_col"))
df = df.select('test_col.*')
df.show(truncate=False)
df.printSchema()
# exit()

df = df.withColumn("map", F.from_json(F.col("body"), schema=nested_schema))
df.show(truncate=False)
df.printSchema()
# exit()

colunas = ("map", "field1", "field2", "field3")
df = df.select(
    F.col("map.field1").alias("field1"),
    F.col("map.field2").alias("field2"),
    F.col("map.field3").alias("field3")
)
df.show(truncate=False)
df.printSchema()

df = df.select(F.col("field1"), F.col("field2"), F.explode(F.col("field3")).alias("field3"))
# df = df.withColumn("new_data", F.explode("data"))
df.show(truncate=False)
df.printSchema()

exit()
# df = df.select("data", F.json_tuple("data", "nome", "telefones", "pedro")).toDF("data", "nome", "telefones", "pedro")
# df.show(truncate=False)
# exit()
# rdd = df.select(F.col("data").alias("jsoncol")).rdd.map(lambda x: x.jsoncol)
# newschema = spark.read.json(rdd).schema


# df = df.select("*", F.from_json("data", newschema).alias("test_col"))

# df = df.select('test_col.*')
colunas = ("body", "field1", "field2", "field3")
df = df.select("body", F.json_tuple(*colunas)).toDF(*colunas)

# df = df.withColumn("telefones", F.array(F.col("telefones")))
# df_with_array = df.withColumn("array_column", F.expr("split(regexp_replace(telefones, '[\\[\\]]', ''), ', ')"))
df.show(truncate=False)
df.printSchema()




exit()

df = (
    df
    .withColumn("data_replaced1", regexp_replace("data", pattern1, r" "))
    .withColumn("data_replaced2", regexp_replace("data_replaced1", pattern2, r"\","))
    .withColumn("data_replaced3", regexp_replace("data_replaced2", pattern3, r"\""))
    .select('data_replaced3')
    .withColumnRenamed("data_replaced3", "data")
    .select("data")
    )


df.show(truncate=False)

df = df.withColumn("data", regexp_replace("data", r'\[', r""))
df = df.withColumn("data", regexp_replace("data", r'\]', r""))
# df = df.withColumn("data", F.concat(F.lit("["), F.col("data"), F.lit("]")))
df.show(truncate=False)
# df = df.withColumn("data", F.array(F.col("data")))
df.show(truncate=False)
df.printSchema()

df = df.withColumn("data", F.split(F.col("data"), ", "))
df.show(truncate=False)
df.printSchema()
exit()
# Explode the array into separate rows
df = df.select(F.explode(F.col("data")).alias("data"))
# df = df.withColumn("new_data", F.explode("data"))
df.show(truncate=False)
df.printSchema()

df = df.select("data", F.json_tuple("data", "pedro", "endereco", "numero")).toDF("data", "pedro", "endereco", "numero")
df.show(truncate=False)