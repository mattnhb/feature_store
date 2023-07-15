import json
from pprint import pprint
from typing import Dict, Any

from pyspark.sql.functions import lit

from data_sanitizer import DataSanitizer
from data_writer import DataWriter
from fake_data import FakeData
from services import get_logger

from pyspark.sql import DataFrame

POSSIBILITIES: Dict[str, Any] = {"fake_data": FakeData}

logger = get_logger(__name__)


class DataContractParser:
    def __init__(self):
        with open("input.json") as json_file:
            self.__content = json.load(json_file)
        # pprint(self.__content)

    def extract(self) -> DataFrame:
        return self.apply_sanitization(
            POSSIBILITIES.get(self.__content["feature_store"])().load_data()
        )

    def apply_sanitization(self, df: DataFrame):
        logger.info(
            "Applying sanitization %s", self.__content.get("sanitization", {})
        )
        df.show()
        df.printSchema()
        return DataSanitizer.sanitize(
            df, sanitization_details=self.__content.get("sanitization", {})
        )

    def create_visions(self):
        df = self.extract()
        df.show()
        DataWriter.save(df, writing_details=self.__content.get("writing"))
