from enum import Enum


class PackageManifestManifestApiVersion(str, Enum):
    EXTENSIONSV1 = "extensions/v1"

    def __str__(self) -> str:
        return str(self.value)
