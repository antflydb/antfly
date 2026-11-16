from enum import StrEnum


class DocumentExtractionRenderConfigResolution(StrEnum):
    MODEL_INPUT = "model_input"
    REQUESTED_DPI = "requested_dpi"

    def __str__(self) -> str:
        return str(self.value)
