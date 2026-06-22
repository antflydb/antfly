from enum import Enum


class ChatToolName(str, Enum):
    ADD_FILTER = "add_filter"
    AGGREGATE = "aggregate"
    ASK_CLARIFICATION = "ask_clarification"
    FETCH = "fetch"
    FULL_TEXT_SEARCH = "full_text_search"
    GRAPH_SEARCH = "graph_search"
    SEMANTIC_SEARCH = "semantic_search"
    TREE_SEARCH = "tree_search"
    WEB_SEARCH = "web_search"

    def __str__(self) -> str:
        return str(self.value)
