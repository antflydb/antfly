from enum import Enum


class ChatToolName(str, Enum):
    ADD_FILTER = "add_filter"
    ASK_CLARIFICATION = "ask_clarification"
    FETCH = "fetch"
    FULL_TEXT_SEARCH = "full_text_search"
    GRAPH_SEARCH = "graph_search"
    SEARCH = "search"
    SEMANTIC_SEARCH = "semantic_search"
    TREE_SEARCH = "tree_search"
    WEBSEARCH = "websearch"

    def __str__(self) -> str:
        return str(self.value)
