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
        (
            df.select(*writing_details.get("columns_to_keep", ["*"]))
            # .repartition(50)
            # .coalesce(1)
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
