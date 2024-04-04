import re

pattern1 = r"(\s'\s)"
pattern2 = r"(,)'|'(,)"
pattern3 = r"(?<=\{)'|'(?=:)|'(?=\})|(\s)'"

data = [
    "{'pedro': 'panda', 'endereco':'casa d ' agua'}",
    "{'pedro': 'panda', 'endereco': 'casa d' agua'}",
    "{'pedro': 'panda','endereco': 'casa d'''' agua'}",
]

for text in data:
    result = re.sub(pattern1, r" ", text)
    result = re.sub(pattern2, r"\",", result)
    result = re.sub(pattern3, r"\"", result)
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
            "{'pedro': 'panda', 'endereco': 'casa d' agua', 'numero': '432'}",
            "{'pedro': 'panda', 'endereco': 'casa d'''' agua', 'numero': '432'}",
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

df = df.select("data", F.json_tuple("data", "pedro", "endereco", "numero")).toDF("data", "pedro", "endereco", "numero")
df.show(truncate=False)