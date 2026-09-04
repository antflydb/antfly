from enum import StrEnum


class InferenceEmbedRequestEncodingFormat(StrEnum):
    FLOAT = "float"

    def __str__(self) -> str:
        return str(self.value)
