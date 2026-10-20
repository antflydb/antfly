from enum import StrEnum


class GraphResolverAdjudicationConfigOnFailure(StrEnum):
    MINT = "mint"
    REVIEW = "review"

    def __str__(self) -> str:
        return str(self.value)
