from enum import StrEnum


class PrivacyProducerType(StrEnum):
    EXTRACTOR = "extractor"
    GENERATOR = "generator"

    def __str__(self) -> str:
        return str(self.value)
