import json
from datetime import date, timedelta
from pprint import pprint
from typing import Dict, Any

from data_writer import DataWriter
from debito.debaggr import DebitoAggregate
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from predicates import PredicateFactory


spark = SparkSession.builder.appName("Create Aggregations").getOrCreate()

# df = spark.read.format("parquet").load("S3")
# df.show(truncate=False)

DAYS_AGO = 50
VISION = "cliente"

POSSIBILITIES: Dict[str, Any] = {
    "debito": DebitoAggregate,
}


def _get_days_ago_predicate(days_ago: int) -> date:
    return date.today() - timedelta(days=days_ago)


class DataContractParser:
    def __init__(self):
        with open("aggregations.json") as json_file:
            self.__content = json.load(json_file)

    def extract(self) -> DataFrame:
        return (
            spark.read.format(self.__content.get("reading").get("format"))
            .load((self.__content.get("reading").get("path")))
            .where(
                (
                    F.col("anomesdia")
                    >= F.lit(_get_days_ago_predicate(DAYS_AGO))
                )
            )
        )

    @staticmethod
    def __add_processing_date_column(df: DataFrame) -> DataFrame:
        return df.withColumn(
            "data_processamento",
            F.date_format(F.current_date(), "yyyy-MM-dd"),
        )

    def apply_aggregations(self) -> DataFrame:
        # pprint(self.__content.get("aggregations", {}).get("general", {}))

        df = POSSIBILITIES.get(
            self.__content.get("feature_store")
        )().create_aggregations(
            df=self.extract(),
            vision=self.__content.get("aggregations", {})
            .get("general", {})
            .get(VISION, {}),
        )

        df = self.__add_processing_date_column(df)
        df.show(truncate=False)
        print(df.columns)
        DataWriter.save(df, writing_details=self.__content.get("writing"))


if __name__ == "__main__":
    DataContractParser().apply_aggregations()
