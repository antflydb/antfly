from enum import StrEnum


class DocumentExtractionReaderConfigProvider(StrEnum):
    ANTFLY = "antfly"
    APPLE = "apple"
    CLOUDFLARE = "cloudflare"
    OPENAI = "openai"
    VERTEX = "vertex"

    def __str__(self) -> str:
        return str(self.value)
