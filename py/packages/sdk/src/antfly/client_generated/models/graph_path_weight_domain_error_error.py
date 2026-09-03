from enum import Enum


class GraphPathWeightDomainErrorError(str, Enum):
    GRAPH_PATH_WEIGHT_DOMAIN_ERROR = "graph_path_weight_domain_error"

    def __str__(self) -> str:
        return str(self.value)
