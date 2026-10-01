from enum import StrEnum


class CatalogMutationVisibilityPendingStatus(StrEnum):
    COMMITTED_VISIBILITY_PENDING = "committed_visibility_pending"

    def __str__(self) -> str:
        return str(self.value)
