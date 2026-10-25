from enum import StrEnum


class VertexEmbedderConfigRequestFormat(StrEnum):
    EMBED_CONTENT = "embed_content"
    PREDICT = "predict"

    def __str__(self) -> str:
        return str(self.value)
