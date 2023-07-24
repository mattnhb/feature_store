from pprint import pprint
import re
import pyspark.sql.functions as F
from operator import and_


def geral(*args, **kwargs):
    return F.lit(True)


def check_pattern(input_string, regex_pattern) -> bool:
    return bool(re.match(regex_pattern, input_string))


def day_number_range(day_range: str):
    start, end = map(int, re.findall(r"\d+", day_range))
    return set(range(start, end + 1))


class PredicateFactory:
    @staticmethod
    def field_equals(key: str, value: str):
        return geral() if key == "geral" else F.col(value) == F.lit(key)

    @staticmethod
    def field_last_n_days(key: str, value: str):
        return F.datediff(F.current_date(), value) <= F.lit(
            int(re.match(r"^ultimos_(\d+)_dias$", key)[1])
        )

    @staticmethod
    def field_date_in_condition(key: str, value: str):
        _predicates = {
            "final_semana": lambda: F.dayofweek(value).isin([1, 7]),
            "noturno": lambda: (F.hour(value) >= 22) | (F.hour(value) < 6),
            "diurno": lambda: ~(F.hour(value) >= 22) | (F.hour(value) < 6),
            "dia_1_8": lambda: (
                F.dayofmonth(value).isin(day_number_range(key))
            ),
            "dia_9_16": lambda: (
                F.dayofmonth(value).isin(day_number_range(key))
            ),
            "dia_17_24": lambda: (
                F.dayofmonth(value).isin(day_number_range(key))
            ),
            "dia_25_31": lambda: (
                F.dayofmonth(value).isin(day_number_range(key))
            ),
            "geral": lambda: geral(),
        }
        return _predicates.get(key, lambda: geral)()

    def dispatcher(self, relations):
        _predicates = {
            "subproduto": self.field_equals,
            "janela": self.field_last_n_days,
            "periodo": self.field_date_in_condition,
        }
        return (
            (_predicates.get(relation)(key, value))
            for relation in relations
            for key, value in relations[relation].items()
        )
