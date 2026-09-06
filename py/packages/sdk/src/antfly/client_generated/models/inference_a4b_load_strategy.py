from enum import StrEnum


class InferenceA4BLoadStrategy(StrEnum):
    AUTO = "auto"
    LEGACY = "legacy"
    PIPELINE = "pipeline"

    def __str__(self) -> str:
        return str(self.value)
