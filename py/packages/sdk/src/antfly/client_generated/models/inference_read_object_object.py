from enum import Enum


class InferenceReadObjectObject(str, Enum):
    READ = "read"

    def __str__(self) -> str:
        return str(self.value)
