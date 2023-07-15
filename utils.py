from functools import reduce
from operator import and_
from typing import List, Any, Dict

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def placeholder(df, *args, **kwargs):
    return df


def filter_reducer(
    df: DataFrame, operator, generator_expression
) -> DataFrame:
    return df.filter(
        reduce(
            operator,
            generator_expression,
        )
    )


def remove_nulls(
    df: DataFrame, fields: List[str], *args, **kwargs
) -> DataFrame:
    return filter_reducer(
        df,
        operator=and_,
        generator_expression=(col(field).isNotNull() for field in fields),
    )


def exclude(
    df: DataFrame, to_exclude: Dict[str, Any], *args, **kwargs
) -> DataFrame:
    return filter_reducer(
        df,
        operator=and_,
        generator_expression=(
            ~col(field).isin(to_exclude.get(field)) for field in to_exclude
        ),
    )


def include_only(
    df: DataFrame, to_include: Dict[str, Any], *args, **kwargs
) -> DataFrame:
    return filter_reducer(
        df,
        operator=and_,
        generator_expression=(
            col(field).isin(to_include.get(field)) for field in to_include
        ),
    )


SANITIZATION_TRANSFORMATIONS = {
    "remove_nulls": remove_nulls,
    "exclude": exclude,
    "include_only": include_only,
}
