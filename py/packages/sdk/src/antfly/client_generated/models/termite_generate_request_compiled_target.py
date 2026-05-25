from enum import Enum


class TermiteGenerateRequestCompiledTarget(str, Enum):
    PARTITIONED = "partitioned"
    WHOLE_MODEL = "whole-model"

    def __str__(self) -> str:
        return str(self.value)
