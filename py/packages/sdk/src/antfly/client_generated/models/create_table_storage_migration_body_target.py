from enum import StrEnum


class CreateTableStorageMigrationBodyTarget(StrEnum):
    VECTOR_STORE = "vector_store"

    def __str__(self) -> str:
        return str(self.value)
