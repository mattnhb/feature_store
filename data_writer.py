from pyspark.sql import SparkSession, DataFrame


class DataWriter:
    @staticmethod
    def save(df: DataFrame, writing_details):
        (
            df.repartition(*writing_details.get("partitions"))
            .write.option(
                "partitionOverwriteMode",
                writing_details.get("partitionOverwriteMode", "dynamic"),
            )
            .format(writing_details.get("saving_format"))
            .mode(writing_details.get("saving_mode"))
            .partitionBy(*writing_details.get("partitions"))
            .save(writing_details.get("saving_path"))
        )
