from enum import StrEnum


class InferenceKvCachePolicyVersion(StrEnum):
    V1 = "v1"

    def __str__(self) -> str:
        return str(self.value)
