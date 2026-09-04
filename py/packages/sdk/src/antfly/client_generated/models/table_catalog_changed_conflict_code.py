from enum import StrEnum


class TableCatalogChangedConflictCode(StrEnum):
    TABLE_CATALOG_CHANGED = "table_catalog_changed"

    def __str__(self) -> str:
        return str(self.value)
