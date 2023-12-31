from typing import List

from faker import Faker
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import Row
from random import choice
from datetime import datetime

# from base.from_origin import BaseOriginData

sc = SparkContext()

spark = SparkSession.builder.appName("Feature Store").getOrCreate()

fake = Faker()
Faker.seed(0)

valores_datas: List[datetime] = [
    fake.date_time_between(start_date="-1y") for _ in range(100)
]
valores_client_id = [fake.uuid4() for _ in range(10)]

valores_produto_origem: List[str] = [
    # "CARDLESS",
    # "COMPRA ECOMMERCE 3DS",
    # "COMPRA INTERNACIONAL CHIP E SENHA",
    # "COMPRA INTERNACIONAL CHIP E SENHA OFFLINE",
    # "COMPRA INTERNACIONAL CONTACTLESS COM SENHA",
    # "COMPRA INTERNACIONAL CONTACTLESS SEM SENHA",
    # "COMPRA INTERNACIONAL TOKEN SEM SENHA",
    # "COMPRA NACIONAL COM CHIP E SENHA",
    # "COMPRA NACIONAL CONTACTLESS COM SENHA",
    # "COMPRA NACIONAL CONTACTLESS INSS COM SENHA",
    # "COMPRA NACIONAL CONTACTLESS INSS SEM SENHA",
    # "COMPRA NACIONAL CONTACTLESS SEM SENHA",
    # "COMPRA NACIONAL INSS COM CHIP E SENHA",
    # "COMPRA NACIONAL TOKEN COM SENHA",
    # "COMPRA NACIONAL TOKEN SEM SENHA",
    # "DEBITO SEM SENHA INTERNACIONAL",
    # "DEBITO SEM SENHA NACIONAL",
    # "M4M INTERNACIONAL",
    # "M4M NACIONAL",
    # "MTT RETENTATIVA",
    # "SAQUE INTERNACIONAL",
    "debitoComAutenticacao",
    "debitoSemAutenticacao",
    "contactlessSemAutentificacao"
]

valores_destino: List[str] = [fake.uuid4() for _ in range(10)]

valores_status_transacao: List[str] = [
    "00",
    "10",
    "11",
    "85",
]


class FakeData:
    def load_data(self):
        return spark.createDataFrame(
            [
                Row(
                    client_id=choice(valores_client_id),
                    data_transacao=date.isoformat(),
                    
                    subproduto=choice(valores_produto_origem),
                    estabelecimento=choice(valores_destino),
                    valor=fake.pyint(),
                    status_transacao=choice(valores_status_transacao),
                    ano=date.year,
                    mes=date.month,
                    dia=date.day,
                )
                for date in valores_datas
            ]
        )


if __name__ == "__main__":
    df = FakeData().load_data()
    df.show(n=5, truncate=False)
    df.select("client_id", "data_transacao", "subproduto", "estabelecimento", "valor").show(n=5, truncate=False)
