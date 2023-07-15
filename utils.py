from typing import List, Any, Dict

from pyspark.sql import DataFrame


def placeholder(df, *args, **kwargs):
    return df


def remove_nulls(
    df: DataFrame, fields: List[str], *args, **kwargs
) -> DataFrame:
    return df.filter(" is not null and ".join(fields) + " is not null")


def exclude(
    df: DataFrame, to_exclude: Dict[str, Any], *args, **kwargs
) -> DataFrame:
    for field in to_exclude:
        print(f"{field} not in {to_exclude.get(field)}")
        df = df.filter(f"{field} not in {to_exclude.get(field)}")
    return df


def include_only(
    df: DataFrame, to_include: Dict[str, Any], *args, **kwargs
) -> DataFrame:
    for field in to_include:
        print(f"{field} in {to_include.get(field)}")
        df = df.filter(f"{field} in {to_include.get(field)}")
    return df


SANITIZATION_TRANSFORMATIONS = {
    "remove_nulls": remove_nulls,
    "exclude": exclude,
    "include_only": include_only,
}
