from enum import StrEnum


class ExaSearchConfigSearchType(StrEnum):
    AUTO = "auto"
    KEYWORD = "keyword"
    NEURAL = "neural"

    def __str__(self) -> str:
        return str(self.value)
