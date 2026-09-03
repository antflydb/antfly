from enum import StrEnum


class GraphResolverConfigCandidateSearch(StrEnum):
    ANN = "ann"
    EXACT_KEY = "exact_key"
    PREFIX = "prefix"
    VALUE_0 = ""

    def __str__(self) -> str:
        return str(self.value)
