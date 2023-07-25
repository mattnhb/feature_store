import json
from datetime import date, timedelta
from pprint import pprint
from typing import Dict, Any

from data_writer import DataWriter
from debito.aggregator import DebitoAggregator
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from utils import add_processing_date_column

# +-------------+---------+----------+---------+-----------------------+--------------+----------+-------+------------------+
# |campo_visao_1|metrica_1|metrica_2 |metrica_3|dimensao_1             |dimensao_2    |dimensao_3|visao  |data_processamento|
# +-------------+---------+----------+---------+-----------------------+--------------+----------+-------+------------------+
# |697bb880-24d |92280    |6591.42857|9719     |debito_com_autenticacao|ultimos_7_dias|diurno    |cliente|2023-07-25        |
# |cdc0b16a-c3f |85349    |5020.52941|9498     |debito_com_autenticacao|ultimos_7_dias|diurno    |cliente|2023-07-25        |
# |3249f8a6-758 |86591    |5411.9375 |9543     |debito_com_autenticacao|ultimos_7_dias|diurno    |cliente|2023-07-25        |
# |cc12c55c-4db |49649    |4137.41666|6995     |debito_com_autenticacao|ultimos_7_dias|diurno    |cliente|2023-07-25        |
# |e524abae-4b6 |77082    |5138.8    |9814     |debito_com_autenticacao|ultimos_7_dias|diurno    |cliente|2023-07-25        |
# |b3954390-72d |71365    |5489.61538|9969     |debito_com_autenticacao|ultimos_7_dias|diurno    |cliente|2023-07-25        |
# +-------------+---------+----------+---------+-----------------------+--------------+----------+-------+------------------+

spark = SparkSession.builder.appName("Create Aggregations").getOrCreate()

VISAO = {
    "client_id": "cliente",
    "estabelecimento": "estabelecimento",
    "client_id-estabelecimento": "cliente_estabelecimento",
}

DAYS_AGO = 50
VISION = "cliente"

POSSIBILITIES: Dict[str, Any] = {
    "debito": DebitoAggregator,
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
            .where((F.col("anomesdia") >= F.lit(_get_days_ago_predicate(DAYS_AGO))))
        )

    def apply_aggregations(self) -> DataFrame:
        df = POSSIBILITIES.get(self.__content.get("feature_store"))(
            VISAO
        ).create_aggregations(
            df=self.extract(),
            vision=self.__content.get("aggregations", {}).get(VISION, {}),
        )

        df = add_processing_date_column(df)
        df.show(truncate=False)
        # DataWriter.save(df, writing_details=self.__content.get("writing"))


if __name__ == "__main__":
    DataContractParser().apply_aggregations()
