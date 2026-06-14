from enum import Enum


class ExaSearchConfigSearchType(str, Enum):
    AUTO = "auto"
    KEYWORD = "keyword"
    NEURAL = "neural"

    def __str__(self) -> str:
        return str(self.value)
