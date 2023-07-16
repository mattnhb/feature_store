from pyspark.sql import DataFrame

from utils import RULES


class DataTransformer:
    @staticmethod
    def transform(df: DataFrame, transformation_details):
        for transformation_rule in transformation_details:
            df = df.transform(
                lambda _df: RULES.get(transformation_rule)(
                    df,
                    transformation_details.get(transformation_rule),
                )
            )
        return df
