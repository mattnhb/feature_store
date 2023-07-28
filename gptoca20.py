import itertools

subproduto = [
    "debito_com_autenticacao",
    "debito_sem_autenticacao",
    "contactless_sem_autentificacao",
    "geral",
]

janela = [
    "ultimos_7_dias",
    "ultimos_30_dias",
    "ultimos_90_dias",
    "ultimos_180_dias",
    "ultimos_270_dias",
    "ultimos_365_dias",
]

periodo = ["diurno", "noturno", "geral"]

metricas = ["countDistinctEstabelecimento"]

combined_list = [
    f"{'.'.join(items)}"
    for items in itertools.product(subproduto, janela, periodo, metricas)
]

print(combined_list)
print(len(combined_list))
