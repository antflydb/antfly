from enum import StrEnum


class MemoryEvaluationStartMode(StrEnum):
    EXTERNAL = "external"
    MANAGED = "managed"
    POLICY = "policy"

    def __str__(self) -> str:
        return str(self.value)
