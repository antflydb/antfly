from enum import Enum


class InferenceGenerateBatchResponseObject(str, Enum):
    GENERATE_BATCH = "generate.batch"

    def __str__(self) -> str:
        return str(self.value)
