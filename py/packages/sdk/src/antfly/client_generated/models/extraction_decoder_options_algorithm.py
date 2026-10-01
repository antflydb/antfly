from enum import StrEnum


class ExtractionDecoderOptionsAlgorithm(StrEnum):
    AUTO = "auto"
    BEAM = "beam"
    EXACT = "exact"

    def __str__(self) -> str:
        return str(self.value)
