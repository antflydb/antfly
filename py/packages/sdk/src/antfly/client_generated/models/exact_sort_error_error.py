from enum import StrEnum


class ExactSortErrorError(StrEnum):
    UNSUPPORTED_EXACT_SORT = "unsupported_exact_sort"

    def __str__(self) -> str:
        return str(self.value)
