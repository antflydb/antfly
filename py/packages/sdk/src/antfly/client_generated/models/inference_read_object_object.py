from enum import StrEnum


class InferenceReadObjectObject(StrEnum):
    READ = "read"

    def __str__(self) -> str:
        return str(self.value)
