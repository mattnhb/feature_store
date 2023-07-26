import json
from functools import reduce
from importlib import import_module
from pprint import pprint
from typing import Dict, Any
from pyspark.sql.functions import lit

from data_sanitizer import DataSanitizer
from data_transformation import DataTransformer
from data_writer import DataWriter
from debito.loader.from_origin import DebitoOriginData
from fakedata.loader.from_origin import FakeData
from pix.loader.from_origin import PixOriginData
from tef.loader.from_origin import TefOriginData
from services import get_logger
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from utils import create_single_date_partition

POSSIBILITIES: Dict[str, Any] = {
    "debito": DebitoOriginData,
    "pix": PixOriginData,
    "tef": TefOriginData,
    "fakedata": FakeData,
}

logger = get_logger(__name__)


class DataContractParser:
    def __init__(self):
        with open("input.json") as json_file:
            self.__content = json.load(json_file)
        # pprint(self.__content)

    def extract(self) -> DataFrame:
        return create_single_date_partition(
            self.apply_extra_transformations(
                self.apply_transformation(
                    self.apply_sanitization(
                        POSSIBILITIES.get(self.__content["feature_store"])().load_data()
                    )
                )
            )
        )

    def apply_transformation(self, df: DataFrame) -> DataFrame:
        print(f"{df.count()=}")
        logger.info(
            "Applying transformations %s",
            self.__content.get("transformations", {}),
        )
        df.show()
        df.printSchema()
        return DataTransformer.transform(
            df,
            transformation_details=self.__content.get("transformations", {}),
        )

    def apply_sanitization(self, df: DataFrame) -> DataFrame:
        print(f"{df.count()=}")
        logger.info("Applying sanitization %s", self.__content.get("sanitization", {}))
        df.show()
        df.printSchema()
        return DataSanitizer.sanitize(
            df, sanitization_details=self.__content.get("sanitization", {})
        )

    def apply_extra_transformations(self, df: DataFrame) -> DataFrame:
        if self.__content.get("extra_transformations", False):
            logger.info("Applying extra transformations")
            module = import_module(
                f"{self.__content['feature_store']}.extra_transformation"
            )
            df = module.ExtraTransformer().apply_extra_transformation(df)
        return df

    def create_snapshots(self):
        df = self.extract()
        df.show(truncate=False)
        df.printSchema()
        print(df.count())
        DataWriter.save(df, writing_details=self.__content.get("writing"))
