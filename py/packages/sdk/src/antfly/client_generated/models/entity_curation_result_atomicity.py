from enum import StrEnum


class EntityCurationResultAtomicity(StrEnum):
    NAMESPACE = "namespace"
    RESOLVER = "resolver"
    SOURCE_TABLE = "source_table"

    def __str__(self) -> str:
        return str(self.value)
