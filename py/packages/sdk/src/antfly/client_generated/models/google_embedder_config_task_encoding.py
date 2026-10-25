from enum import StrEnum


class GoogleEmbedderConfigTaskEncoding(StrEnum):
    NONE = "none"
    PROMPT = "prompt"
    TASK_TYPE = "task_type"

    def __str__(self) -> str:
        return str(self.value)
