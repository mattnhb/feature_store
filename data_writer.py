from pyspark.sql import SparkSession, DataFrame
from services import get_logger

logger = get_logger(__name__)


class DataWriter:
    @staticmethod
    def save(df: DataFrame, writing_details):
        logger.info(
            "Writing your data with the following configuration: %s",
            writing_details,
        )
        threshold = 900
        partition_factor = max(1, (df.count() // (threshold // 2)))
        df = (
            df.coalesce(1)
            if partition_factor == 1
            else df.repartition(partition_factor)
        )
        print(f"{df.count()=} {partition_factor=}")
        (
            df.select(*writing_details.get("columns_to_keep", ["*"]))
            # .repartition(*writing_details.get("partitions"))
            .write.option(
                "partitionOverwriteMode",
                writing_details.get("partitionOverwriteMode", "dynamic"),
            )
            .option("ignoreNullFields", False)
            .format(writing_details.get("saving_format"))
            .mode(writing_details.get("saving_mode"))
            .partitionBy(*writing_details.get("partitions"))
            .save(writing_details.get("saving_path"))
        )
