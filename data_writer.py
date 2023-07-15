from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Feature Store").getOrCreate()


class DataWriter:
    def save(self, df):
        (
            df.repartition(*["ano", "mes", "dia"])
            .write.format("parquet")
            .mode("overwrite")
            .partitionBy(*["ano", "mes", "dia"])
            .save("S3")
        )
