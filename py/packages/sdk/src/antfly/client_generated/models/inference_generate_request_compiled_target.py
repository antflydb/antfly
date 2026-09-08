from enum import StrEnum


class InferenceGenerateRequestCompiledTarget(StrEnum):
    PARTITIONED = "partitioned"
    WHOLE_MODEL = "whole-model"

    def __str__(self) -> str:
        return str(self.value)
