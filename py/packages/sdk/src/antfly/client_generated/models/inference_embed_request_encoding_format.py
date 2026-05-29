from enum import Enum


class InferenceEmbedRequestEncodingFormat(str, Enum):
    FLOAT = "float"

    def __str__(self) -> str:
        return str(self.value)
