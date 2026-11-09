from enum import StrEnum


class DocumentExtractionRenderConfigSampling(StrEnum):
    AUTO = "auto"
    PDF = "pdf"
    SMOOTH = "smooth"

    def __str__(self) -> str:
        return str(self.value)
