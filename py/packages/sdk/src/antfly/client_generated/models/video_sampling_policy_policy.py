from enum import StrEnum


class VideoSamplingPolicyPolicy(StrEnum):
    REFERENCE = "reference"
    TIMESTAMPS = "timestamps"

    def __str__(self) -> str:
        return str(self.value)
