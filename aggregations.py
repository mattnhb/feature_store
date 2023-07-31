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
from functools import reduce
import json
from data_writer import DataWriter
from pyspark.sql import functions as F
from pyspark.sql import Row
from faker import Faker
from random import choice
from pyspark.sql.window import Window
from pprint import pprint
from debito.jsonify_handler import DebitoJsonifyHandler

fake = Faker()
Faker.seed(0)

from pyspark.sql import SparkSession
from typing import Dict, Any, Iterable, Tuple
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType

POSSIBILITIES2 = {"debito": DebitoJsonifyHandler}


def create_nested_dict(input_dict: Dict[str, Any]) -> Dict[str, Any]:
    def add_to_nested_dict(
        result: Dict[str, Any], key_value: Iterable[Tuple[str, Any]]
    ) -> Dict[str, Any]:
        key, value = key_value
        keys = key.split("#")
        last_key = keys.pop()
        reduce(lambda d, k: d.setdefault(k, {}), keys, result)[last_key] = value
        return result

    return reduce(add_to_nested_dict, input_dict.items(), {})


def nested_to_json(nested_dict, metrics_default_values):
    def convert_to_json(d):
        if isinstance(d, dict):
            return F.struct(
                *[convert_to_json(d[dimension]).alias(dimension) for dimension in d]
            )
        return F.coalesce(
            d, F.lit(metrics_default_values.get(d._jc.toString().rsplit("_", 1)[1], 0))
        )

    return convert_to_json(nested_dict)


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

spark = (
    SparkSession.builder.appName("Create Aggregations")
    # .config("spark.sql.shuffle.partitions", 400)
    # .config("spark.default.parallelism", 400)
    .config("spark.executor.cores", "6")
    .config("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "1024")
    .getOrCreate()
)
spark.sparkContext.setCheckpointDir("check_agg")
VISAO = {
    "client_id": "cliente",
    "estabelecimento": "estabelecimento",
    "client_id-estabelecimento": "cliente_estabelecimento",
}

DAYS_AGO = 365

POSSIBILITIES: Dict[str, Any] = {
    "debito": DebitoAggregator,
}


def _get_days_ago_predicate(days_ago: int) -> date:
    return date.today() - timedelta(days=days_ago)


class DataContractParser:
    def __init__(self, vision):
        self.vision = vision
        with open(f"aggregations_{vision}.json") as json_file:
            self.__content = json.load(json_file)

    def extract(self) -> DataFrame:
        return (
            spark.read.format(self.__content.get("reading").get("format"))
            .load((self.__content.get("reading").get("path")))
            .where((F.col("anomesdia") >= F.lit(_get_days_ago_predicate(DAYS_AGO))))
            # .repartition(200)
        )#.cache()

    def apply_aggregations(self) -> DataFrame:
        df = POSSIBILITIES.get(self.__content.get("feature_store"))(
            vision=self.__content.get("aggregations", {}).get(self.vision, {}),
        ).create_aggregations(
            df=self.extract(),
        )
        # df.checkpoint()
        df = jsonify(df, self.vision)
        df = add_processing_date_column(df)
        # print(f"{df.rdd.getNumPartitions()=}")
        # df.show(truncate=False)
        print("bora escrever")
        # print(f"{df.count()}")
        # df.cache()
        DataWriter.save(df, writing_details=self.__content.get("writing"))


def jsonify(df, vision):
    handler = POSSIBILITIES2.get("debito")(vision)

    # dx = (
    #     spark.read.format("json")
    #     .load(f"AGGREGATED/visao={vision}/")
    #     .filter(F.col("data_processamento") == "2023-07-29")
    #     .drop("data_processamento")
    # )
    # print(f"{vision=}")
    # dx = df.cache().persist()
    dx = df  # .cache()
    first_colunas = list(
        filter(
            lambda coluna: coluna
            not in {*handler.dimension_names, *handler.grouped_by},
            dx.columns,
        )
    )
    # print(f"{first_colunas=}")

    dx = dx.withColumn(
        "_pivot",
        F.concat_ws("#", *handler.dimension_names),
    )

    dx = (
        dx.groupBy(*handler.grouped_by)
        .pivot("_pivot")
        .agg(
            *[
                F.first(
                    metric,
                    ignorenulls=(True if metric in handler.specific_metrics else False),
                ).alias(metric)
                for metric in first_colunas
            ]
        )
    )

    dx = dx.drop(*[])
    colunas = list(
        filter(lambda coluna: coluna not in {*handler.grouped_by}, dx.columns)
    )

    rel_col = {"#".join(coluna.rsplit("_", 1)): F.col(coluna) for coluna in colunas}
    nested = create_nested_dict(rel_col)
    # dx.printSchema()

    dx = dx.withColumn(
        "metricas",
        nested_to_json(nested, metrics_default_values=handler.metrics_default_values),
    )
    dx = handler.to_dynamo_schema(dx)
    return dx
    dx.show()
    dx.printSchema()
    exit()


if __name__ == "__main__":
    for vision in ("cliente", "estabelecimento", "cliente_estabelecimento"):
        DataContractParser(vision).apply_aggregations()
