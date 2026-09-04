from enum import StrEnum


class MergeStrategy(StrEnum):
    FAILOVER = "failover"
    RRF = "rrf"
    RSF = "rsf"

    def __str__(self) -> str:
        return str(self.value)
