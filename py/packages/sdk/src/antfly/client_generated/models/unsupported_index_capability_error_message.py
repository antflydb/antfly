from enum import StrEnum


class UnsupportedIndexCapabilityErrorMessage(StrEnum):
    ARTIFACT_BACKED_INDEX_SOURCES_ARE_NOT_SUPPORTED_BY_THIS_DEPLOYMENT = (
        "artifact-backed index sources are not supported by this deployment"
    )

    def __str__(self) -> str:
        return str(self.value)
