from collections import defaultdict
from functools import reduce
from operator import and_
from typing import List, Any, Dict, Callable, Union

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def union_frames(frames: List[DataFrame]) -> DataFrame:
    return reduce(
        lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True),
        frames,
    ).distinct()


def create_single_date_partition(df: DataFrame) -> DataFrame:
    return reduce(
        lambda _df, col: _df.withColumn(col, F.lpad(col, 2, "0")),
        {"mes", "dia"},
        df,
    ).withColumn("anomesdia", F.concat_ws("-", "ano", "mes", "dia"))


def add_processing_date_column(
    df: DataFrame, column_name: str = "data_processamento"
) -> DataFrame:
    return df.withColumn(
        column_name,
        F.date_format(F.current_date(), "yyyy-MM-dd"),
    )


def get_vision_name(grouped_columns, vision_relation):
    return (
        vision_relation.get("-".join(grouped_columns))
        if len(grouped_columns) > 1
        else vision_relation.get(*grouped_columns)
    )


def _placeholder(df, *args, **kwargs):
    return df


def _filter_reducer(df: DataFrame, operator, generator_expression) -> DataFrame:
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
    def remove_nulls(df: DataFrame, fields: List[str], *args, **kwargs) -> DataFrame:
        return _filter_reducer(
            df,
            operator=and_,
            generator_expression=(F.col(field).isNotNull() for field in fields),
        )

    @staticmethod
    def exclude(
        df: DataFrame, to_exclude: Dict[str, Any], *args, **kwargs
    ) -> DataFrame:
        return _filter_reducer(
            df,
            operator=and_,
            generator_expression=(
                ~F.col(field).isin(to_exclude.get(field)) for field in to_exclude
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
                F.col(field).isin(to_include.get(field)) for field in to_include
            ),
        )


class __Transformation:
    @staticmethod
    def rename(df: DataFrame, to_rename: Dict[str, Any], *args, **kwargs) -> DataFrame:
        return with_column_reducer(
            callable_=lambda _df, column_name: _df.withColumnRenamed(
                column_name, to_rename.get(column_name)
            ),
            collection=to_rename,
            df=df,
        )

    @staticmethod
    def cast(df: DataFrame, to_cast: Dict[str, Any], *args, **kwargs) -> DataFrame:
        return with_column_reducer(
            callable_=lambda _df, column_name: _df.withColumn(
                column_name, _df[column_name].cast(to_cast.get(column_name))
            ),
            collection=to_cast,
            df=df,
        )

    @staticmethod
    def case_when(
        df: DataFrame, conditions: Dict[str, Any], *args, **kwargs
    ) -> DataFrame:
        return with_column_reducer(
            callable_=lambda _df, column_name: _df.withColumn(
                column_name,
                F.expr(
                    f"""
                        case {"".join(
                        f"when {when} then {repr(then)} "
                        for then, when in conditions.get(column_name, {}).items() if then != "else"
                    ) + ("else " + repr(else_present) if (
                        else_present := conditions.get(column_name, {}).pop("else", False)) else "")} end"""
                ),
            ),
            collection=conditions,
            df=df,
        )

    @staticmethod
    def day_of_week(
        df: DataFrame, data_columns: Dict[str, Any], *args, **kwargs
    ) -> DataFrame:
        return with_column_reducer(
            callable_=lambda _df, column_name: _df.withColumn(
                data_columns.get(column_name), F.dayofweek(column_name)
            ),
            collection=data_columns,
            df=df,
        )


RULES = defaultdict(
    _placeholder,
    {
        "remove_nulls": __Sanitization.remove_nulls,
        "exclude": __Sanitization.exclude,
        "include_only": __Sanitization.include_only,
        "rename": __Transformation.rename,
        "cast": __Transformation.cast,
        "case_when": __Transformation.case_when,
        "extract_dayofweek": __Transformation.day_of_week,
        "columns_to_keep": "*",
    },
)
