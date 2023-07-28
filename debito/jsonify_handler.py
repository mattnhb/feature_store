from base.jsonify_handler import BaseJsonifyHandler
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

relation = {
    "cliente": {
        "grouped_by": ["client_id"],
        "specific_metrics": [],
        "dimension_names": ["subproduto", "janela", "periodo"],
    },
    "estabelecimento": {
        "grouped_by": ["estabelecimento"],
        "specific_metrics": [],
        "dimension_names": ["subproduto", "janela", "periodo"],
    },
    "cliente_estabelecimento": {
        "grouped_by": ["client_id", "estabelecimento"],
        "specific_metrics": [],
        "dimension_names": ["subproduto", "janela", "periodo"],
    },
}

metrics_default_values = {"d6Valor": "pedro"}


class DebitoJsonifyHandler(BaseJsonifyHandler):
    def __init__(self, vision: str) -> None:
        self.vision_name = vision
        self.grouped_by = relation[vision]["grouped_by"]
        self.specific_metrics = relation[vision]["specific_metrics"]
        self.dimension_names = relation[vision]["dimension_names"]
        self.metrics_default_values = metrics_default_values

    def to_dynamo_schema(self, df: DataFrame) -> DataFrame:
        return (
            df.withColumn("cod_chav_patc", F.concat_ws("#", *self.grouped_by))
            .withColumn("cod_orde_tran", F.lit(self.vision_name))
            .withColumn("visao", F.lit(self.vision_name))
            .select("cod_chav_patc", "cod_orde_tran", "metricas", "visao")
        )
