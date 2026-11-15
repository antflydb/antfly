from enum import StrEnum


class PermissionType(StrEnum):
    ADMIN = "admin"
    CURATOR = "curator"
    READ = "read"
    REPLICATE = "replicate"
    WRITE = "write"

    def __str__(self) -> str:
        return str(self.value)
