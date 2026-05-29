from enum import Enum


class InferenceDocumentTokenClassificationObjectObject(str, Enum):
    DOCUMENT_TOKEN_CLASSIFICATION = "document.token_classification"

    def __str__(self) -> str:
        return str(self.value)
