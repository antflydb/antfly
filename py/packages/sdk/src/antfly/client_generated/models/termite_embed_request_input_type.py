from enum import Enum


class TermiteEmbedRequestInputType(str, Enum):
    CLASSIFICATION = "classification"
    CLUSTERING = "clustering"
    DOCUMENT = "document"
    QUERY = "query"
    SEARCH_DOCUMENT = "search_document"
    SEARCH_QUERY = "search_query"

    def __str__(self) -> str:
        return str(self.value)
