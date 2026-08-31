from enum import Enum


class ExactSortErrorError(str, Enum):
    UNSUPPORTED_EXACT_SORT = "unsupported_exact_sort"

    def __str__(self) -> str:
        return str(self.value)
