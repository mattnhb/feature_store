import json
from pprint import pprint
from typing import Dict, Any
from fake_data import FakeData
from services import get_logger
from utils import SANITIZATION_TRANSFORMATIONS
from pyspark.sql import DataFrame

POSSIBILITIES: Dict[str, Any] = {"fake_data": FakeData}

logger = get_logger(__name__)


class DataSourceParser:
    def __init__(self):
        with open("input.json") as json_file:
            self.__content = json.load(json_file)
        pprint(self.__content)

    def extract(self) -> DataFrame:
        return self.apply_sanitization(
            POSSIBILITIES.get(self.__content["feature_store"])().load_data()
        )

    def apply_sanitization(self, df: DataFrame):
        logger.info("Applying sanitization")
        df.show()
        df.printSchema()
        for sanitization_rule in self.__content.get("sanitization", {}):
            df = df.transform(
                lambda _df: SANITIZATION_TRANSFORMATIONS.get(
                    sanitization_rule
                )(
                    df,
                    self.__content.get("sanitization", {}).get(
                        sanitization_rule
                    ),
                )
            )

        return df
