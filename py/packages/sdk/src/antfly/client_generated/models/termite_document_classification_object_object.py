from enum import Enum


class TermiteDocumentClassificationObjectObject(str, Enum):
    DOCUMENT_CLASSIFICATION = "document.classification"

    def __str__(self) -> str:
        return str(self.value)
