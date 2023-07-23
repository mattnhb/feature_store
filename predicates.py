from pprint import pprint
import re
import pyspark.sql.functions as F
from operator import and_


def geral(*args, **kwargs):
    return F.lit(True)


class PredicateFactory:
    @staticmethod
    def field_equals(key: str, value: str):
        return geral() if key == "geral" else F.col(value) == F.lit(key)

    @staticmethod
    def field_last_n_days(key: str, value: str):
        return F.datediff(F.current_date(), value) <= F.lit(
            int(re.match(r"^ultimos_(\d+)_dias$", key)[1])
        )

    # {'janela': {'ultimos_180_dias': 'data_transacao'},
    #  'periodo': {'diurno': 'data_transacao'},
    #  'subproduto': {'geral': 'subproduto'}},

    def dispatcher(self, relations):
        print(f"Creating predicate for {relations}")
        _predicates = {
            "subproduto": self.field_equals,
            "janela": self.field_last_n_days,
            "periodo": geral,
        }
        return (
            (_predicates.get(relation)(key, value))
            for relation in relations
            for key, value in relations[relation].items()
        )


# POSSIBILITIES: Dict[str, Any] = {
#     "debito": DebitoOriginData,
#     "pix": PixOriginData,
#     "tef": TefOriginData,
#     "fakedata": FakeData,
# }
