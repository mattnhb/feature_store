from itertools import product
from pprint import pprint

input_dict = {
    "janela": [
        {"ultimos_7_dias": "data_transacao"},
        {"ultimos_30_dias": "data_transacao"},
        {"ultimos_90_dias": "data_transacao"},
        {"ultimos_180_dias": "data_transacao"},
        {"ultimos_270_dias": "data_transacao"},
        {"ultimos_365_dias": "data_transacao"},
    ],
    "periodo": [
        {"diurno": "data_transacao"},
        {"noturno": "data_transacao"},
        {"final_semana": "data_transacao"},
        {"geral": "data_transacao"},
    ],
    "subproduto": [
        {"debito_com_autenticacao": "subproduto"},
        {"debito_sem_autenticacao": "subproduto"},
        {"contactless_sem_autentificacao": "subproduto"},
        {"geral": "subproduto"},
    ],
}

# Get the keys and values from the input_dict
keys = input_dict.keys()
values = list(input_dict.values())

# Generate all possible combinations using itertools.product
combinations = [
    dict(zip(keys, combination)) for combination in product(*values)
]

# Print a short example of the output
pprint(len(combinations))
pprint(combinations)  # Print the first combination
