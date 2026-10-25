from enum import StrEnum


class VertexEmbedderConfigTaskEncoding(StrEnum):
    NONE = "none"
    PROMPT = "prompt"
    TASK_TYPE = "task_type"

    def __str__(self) -> str:
        return str(self.value)
