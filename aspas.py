import re

pattern_none = r'None'
pattern0 = r'"'
pattern1 = r"(\s'\s)"
pattern2 = r"(,)'|'(,)"
pattern3 = r"(?<=\{)'|'(?=:)|'(?=\})|(\s)'"

data = [
    '{"None": "panda", "endereco":"casa d " agua"}',
    "{'pedro': 'None', 'endereco': 'casa d' agua'}",
    "[{'pedro': 'panda','endereco': 'casa d'''' agua'}]",
    "None"

]

for text in data:
    
    result = re.sub(pattern0, r'\n', text)
    result = re.sub(pattern1, r" ", text)
    result = re.sub(pattern2, r"\",", result)
    result = re.sub(pattern3, r"\"", result)
    result = re.sub(pattern_none, 'null', text)
    print(result)

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
from pyspark.sql import Row
from pyspark.sql import functions as F


spark = SparkSession.builder.appName("Regex Replacement").getOrCreate()



df = spark.createDataFrame(
    [
        Row(data=_)
        for _ in [
            "{'pedro': 'panda', 'endereco': 'casa d ' agua', 'numero': '432'}",
            "{'pedro': 'panda', 'endereco': 'casa d' agua', 'numero': '432'}, {'pedro': '321', 'endereco': '213 d' agua', 'numero': '132'}",
            "[{'pedro': 'panda', 'endereco': 'casa d'''' agua', 'numero': '432'}]",
            "None"
        ]
    ]
)


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

# Explode the array into separate rows
df = df.select(F.explode(F.col("data")).alias("data"))
# df = df.withColumn("new_data", F.explode("data"))
df.show(truncate=False)
df.printSchema()

df = df.select("data", F.json_tuple("data", "pedro", "endereco", "numero")).toDF("data", "pedro", "endereco", "numero")
df.show(truncate=False)
