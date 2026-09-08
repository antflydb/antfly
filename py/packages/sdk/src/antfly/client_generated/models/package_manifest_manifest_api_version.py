from enum import StrEnum


class PackageManifestManifestApiVersion(StrEnum):
    EXTENSIONSV1 = "extensions/v1"

    def __str__(self) -> str:
        return str(self.value)
