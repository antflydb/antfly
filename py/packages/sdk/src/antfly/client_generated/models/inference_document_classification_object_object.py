from enum import Enum


class InferenceDocumentClassificationObjectObject(str, Enum):
    DOCUMENT_CLASSIFICATION = "document.classification"

    def __str__(self) -> str:
        return str(self.value)
