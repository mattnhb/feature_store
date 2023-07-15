import json
from pprint import pprint
from typing import Dict, Any
from fake_data import FakeData
from utils import SANITIZATION_TRANSFORMATIONS

POSSIBILITIES: Dict[str, Any] = {"fake_data": FakeData}


class DataSourceParser:
    def __init__(self):
        with open("input.json") as json_file:
            self.__content = json.load(json_file)
        pprint(self.__content)

    def extract(self):
        return self.apply_sanitization(
            POSSIBILITIES.get(self.__content["feature_store"])().load_data()
        )

    def apply_sanitization(self, df):
        df.show()
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
