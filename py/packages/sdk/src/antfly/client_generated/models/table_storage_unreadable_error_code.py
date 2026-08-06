from enum import Enum


class TableStorageUnreadableErrorCode(str, Enum):
    TABLE_STORAGE_UNREADABLE = "table_storage_unreadable"

    def __str__(self) -> str:
        return str(self.value)
