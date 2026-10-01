from enum import StrEnum


class ExtractionClassificationSchemaActivation(StrEnum):
    AUTO = "auto"
    SIGMOID = "sigmoid"
    SOFTMAX = "softmax"

    def __str__(self) -> str:
        return str(self.value)
