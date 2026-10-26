from enum import StrEnum


class InferenceContextCheckpointVersion(StrEnum):
    V1 = "v1"

    def __str__(self) -> str:
        return str(self.value)
