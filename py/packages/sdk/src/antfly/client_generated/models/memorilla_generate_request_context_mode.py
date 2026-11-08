from enum import StrEnum


class MemorillaGenerateRequestContextMode(StrEnum):
    LATENT = "latent"
    SOURCE_VERIFIED = "source_verified"

    def __str__(self) -> str:
        return str(self.value)
