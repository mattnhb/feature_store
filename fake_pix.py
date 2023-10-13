from datetime import timedelta, timezone

from faker import Factory
from faker.providers import DynamicProvider
from faker.providers.ssn.pt_BR import Provider as BRProvider
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

codigo_situacao_consolidada_transferencia_provider = DynamicProvider(
    provider_name="codigo_situacao_consolidada_transferencia",
    elements=["ACSC", "RJCT"],
)
codigo_canal_distribuicao_provider = DynamicProvider(
    provider_name="codigo_canal_distribuicao",
    elements=["C1", "71", "7E", "96", "06", "40"],
)


spark = SparkSession.builder.appName("Fake Data").getOrCreate()
faker = Factory.create()
faker.add_provider(BRProvider)
faker.add_provider(codigo_situacao_consolidada_transferencia_provider)
faker.add_provider(codigo_canal_distribuicao_provider)


def fake_name():
    return faker.name()


def fake_cpf_origem():
    return faker.cpf().replace("-", "").replace(".", "")


def fake_cpf_destino():
    return faker.cpf().replace("-", "").replace(".", "")[::-1]


def fake_anomesdia():
    return faker.date_time_between("-1y").strftime("%Y%m%d")


def fake_data_hora_transacao_financeira():
    _temp_date = (
        faker.date_time_between("-1y")
        .replace(tzinfo=timezone(timedelta(hours=-3)))
        .strftime("%Y-%m-%dT%H:%M:%S.%f%z")
    )
    return _temp_date[:-2] + ":" + _temp_date[-2:]


fake_name_udf = udf(fake_name, StringType())
fake_cpf_origem_udf = udf(fake_cpf_origem, StringType())
fake_cpf_destino_udf = udf(fake_cpf_destino, StringType())
fake_codigo_situacao_consolidada_transferencia_udf = udf(
    faker.codigo_situacao_consolidada_transferencia, StringType()
)
fake_codigo_canal_distribuicao_udf = udf(faker.codigo_canal_distribuicao, StringType())
fake_anomesdia_udf = udf(fake_anomesdia, StringType())
fake_data_hora_transacao_financeira_udf = udf(
    fake_data_hora_transacao_financeira, StringType()
)

df = spark.createDataFrame([Row(index=n) for n in range(8_000)])
df = (
    df.withColumn("NUMERO_CPF_CNPJ_ORIGEM", fake_cpf_origem_udf())
    .withColumn("NUMERO_CPF_CNPJ_DESTINO", fake_cpf_destino_udf())
    .withColumn("CODIGO_CANAL_DISTRIBUICAO", fake_codigo_canal_distribuicao_udf())
    .withColumn(
        "CODIGO_SITUACAO_CONSOLIDADA_TRANSFERENCIA",
        fake_codigo_situacao_consolidada_transferencia_udf(),
    )
    .withColumn(
        "VALOR_TRANSACAO_FINANCEIRA", udf(lambda: faker.pyfloat(min_value=0.1))()
    )
    .withColumn("anomesdia", fake_anomesdia_udf())
    .withColumn(
        "DATA_HORA_TRANSACAO_FINANCEIRA", fake_data_hora_transacao_financeira_udf()
    )
    .drop("index")
)

# df = df.withColumn("medical", fake_medical_udf())
df.show(truncate=False)
