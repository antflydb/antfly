from enum import StrEnum


class LakeBulkBuildAlgorithm(StrEnum):
    EMEANS = "emeans"
    LLOYD = "lloyd"

    def __str__(self) -> str:
        return str(self.value)
