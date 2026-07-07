from enum import Enum


class SortProfileNativeFilterMode(str, Enum):
    DOC_IDS = "doc_ids"
    DOC_NUMS = "doc_nums"
    EMPTY = "empty"
    EXCLUSION_ONLY = "exclusion_only"
    MIXED = "mixed"
    NONE = "none"

    def __str__(self) -> str:
        return str(self.value)
