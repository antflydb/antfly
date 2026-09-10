from enum import StrEnum


class TableStorageSettingsDenseEmbeddings(StrEnum):
    PRIMARY_LSM = "primary_lsm"
    VECTOR_STORE = "vector_store"

    def __str__(self) -> str:
        return str(self.value)
