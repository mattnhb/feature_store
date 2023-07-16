from functools import reduce
from operator import and_
from typing import List, Any, Dict, Callable, Union

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def _placeholder(df, *args, **kwargs):
    return df


def _filter_reducer(
    df: DataFrame, operator, generator_expression
) -> DataFrame:
    return df.filter(
        reduce(
            operator,
            generator_expression,
        )
    )


def with_column_reducer(
    callable_: Callable, collection: Union[List, Dict], df: DataFrame
) -> DataFrame:
    return reduce(
        callable_,
        collection,
        df,
    )


class __Sanitization:
    @staticmethod
    def remove_nulls(
        df: DataFrame, fields: List[str], *args, **kwargs
    ) -> DataFrame:
        return _filter_reducer(
            df,
            operator=and_,
            generator_expression=(col(field).isNotNull() for field in fields),
        )

    @staticmethod
    def exclude(
        df: DataFrame, to_exclude: Dict[str, Any], *args, **kwargs
    ) -> DataFrame:
        return _filter_reducer(
            df,
            operator=and_,
            generator_expression=(
                ~col(field).isin(to_exclude.get(field))
                for field in to_exclude
            ),
        )

    @staticmethod
    def include_only(
        df: DataFrame, to_include: Dict[str, Any], *args, **kwargs
    ) -> DataFrame:
        return _filter_reducer(
            df,
            operator=and_,
            generator_expression=(
                col(field).isin(to_include.get(field)) for field in to_include
            ),
        )


class __Transformation:
    @staticmethod
    def rename(
        df: DataFrame, to_rename: Dict[str, Any], *args, **kwargs
    ) -> DataFrame:
        return with_column_reducer(
            callable_=lambda _df, column_name: _df.withColumnRenamed(
                column_name, to_rename.get(column_name)
            ),
            collection=to_rename,
            df=df,
        )

    @staticmethod
    def cast(
        df: DataFrame, to_cast: Dict[str, Any], *args, **kwargs
    ) -> DataFrame:
        return with_column_reducer(
            callable_=lambda _df, column_name: _df.withColumn(
                column_name, _df[column_name].cast(to_cast.get(column_name))
            ),
            collection=to_cast,
            df=df,
        )


RULES: Dict[str, Callable] = {
    "remove_nulls": __Sanitization.remove_nulls,
    "exclude": __Sanitization.exclude,
    "include_only": __Sanitization.include_only,
    "rename": __Transformation.rename,
    "cast": __Transformation.cast,
}
