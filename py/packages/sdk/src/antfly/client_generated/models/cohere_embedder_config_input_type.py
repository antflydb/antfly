from enum import StrEnum


class CohereEmbedderConfigInputType(StrEnum):
    CLASSIFICATION = "classification"
    CLUSTERING = "clustering"
    SEARCH_DOCUMENT = "search_document"
    SEARCH_QUERY = "search_query"

    def __str__(self) -> str:
        return str(self.value)
