from enum import StrEnum


class MemorillaGenerateResponseContextSource(StrEnum):
    SUPPLIED_EMBEDDINGS = "supplied_embeddings"
    SUPPLIED_EMBEDDINGS_AND_DOCUMENTS = "supplied_embeddings_and_documents"

    def __str__(self) -> str:
        return str(self.value)
