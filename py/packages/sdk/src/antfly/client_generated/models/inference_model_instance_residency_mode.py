from enum import StrEnum


class InferenceModelInstanceResidencyMode(StrEnum):
    BACKEND_MANAGED = "backend_managed"
    RESIDENT = "resident"
    STREAMED = "streamed"

    def __str__(self) -> str:
        return str(self.value)
