from enum import Enum


class InferenceClassifyObjectObject(str, Enum):
    CLASSIFICATION = "classification"

    def __str__(self) -> str:
        return str(self.value)
