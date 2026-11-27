from enum import StrEnum


class EnrichmentSeedMappingOnMissing(StrEnum):
    FAIL = "fail"
    GENERATE = "generate"
    PENDING = "pending"

    def __str__(self) -> str:
        return str(self.value)
