from enum import StrEnum


class InferenceModelRuntimeResponseObject(StrEnum):
    MODEL_RUNTIME = "model_runtime"

    def __str__(self) -> str:
        return str(self.value)
