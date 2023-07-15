from pyspark.sql import DataFrame

from utils import SANITIZATION_TRANSFORMATIONS


class DataSanitizer:
    @staticmethod
    def sanitize(df: DataFrame, sanitization_details) -> DataFrame:
        for sanitization_rule in sanitization_details:
            df = df.transform(
                lambda _df: SANITIZATION_TRANSFORMATIONS.get(
                    sanitization_rule
                )(
                    df,
                    sanitization_details.get(sanitization_rule),
                )
            )
        return df
