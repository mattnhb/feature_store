import json
from datetime import date, timedelta
from pprint import pprint
from typing import Dict, Any
from debito.debaggr import DebitoAggregate
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from predicates import PredicateFactory


spark = SparkSession.builder.appName("Create Aggregations").getOrCreate()

# df = spark.read.format("parquet").load("S3")
# df.show(truncate=False)

DAYS_AGO = 365
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

    def apply_aggregations(self) -> DataFrame:
        # pprint(self.__content.get("aggregations", {}).get("general", {}))

        self.extract()
        return POSSIBILITIES.get(
            self.__content.get("feature_store")
        )().create_aggregations(
            df=self.extract(),
            vision=self.__content.get("aggregations", {})
            .get("general", {})
            .get(VISION, {}),
        )


if __name__ == "__main__":
    pf = PredicateFactory()
    df = DataContractParser().apply_aggregations()
    (
        df
        # .where(
        #     pf.field_equals(
        #         value="contactless_sem_autentificacao", field="subproduto"
        #     )
        #     & pf.field_last_n_days(n_days=10, field="data_transacao")
        # )
        # .select("anomesdia", "subproduto")
        # .distinct()
        .show(truncate=False)
    )
