from enum import StrEnum


class VideoSamplingPolicyOverflow(StrEnum):
    TRUNCATE = "truncate"
    UNIFORM = "uniform"

    def __str__(self) -> str:
        return str(self.value)
