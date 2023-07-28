relation = {
    "cliente": {
        "grouped_by": ["client_id"],
        "specific_metrics": [],
        "dimension_names": ["subproduto", "janela", "periodo"],
    },
    "estabelecimento": {
        "grouped_by": ["estabelecimento"],
        "specific_metrics": [],
        "dimension_names": ["subproduto", "janela", "periodo"],
    },
    "cliente_estabelecimento": {
        "grouped_by": ["client_id", "estabelecimento"],
        "specific_metrics": [],
        "dimension_names": ["subproduto", "janela", "periodo"],
    },
}


class DebitoJsonifyHandler:
    def __init__(self, vision: str) -> None:
        self.grouped_by = relation[vision]["grouped_by"]
        self.specific_metrics = relation[vision]["specific_metrics"]
        self.dimension_names = relation[vision]["dimension_names"]
