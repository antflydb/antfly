from enum import StrEnum


class InferenceGenerateBatchResponseObject(StrEnum):
    GENERATE_BATCH = "generate.batch"

    def __str__(self) -> str:
        return str(self.value)
