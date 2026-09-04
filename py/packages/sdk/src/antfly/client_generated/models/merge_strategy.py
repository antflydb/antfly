from enum import StrEnum


class MergeStrategy(StrEnum):
    RRF = "rrf"
    RSF = "rsf"

    def __str__(self) -> str:
        return str(self.value)
