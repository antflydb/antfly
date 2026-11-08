from enum import StrEnum


class DocumentExtractionConfigChecksumAlgorithm(StrEnum):
    SHA256 = "sha256"

    def __str__(self) -> str:
        return str(self.value)
