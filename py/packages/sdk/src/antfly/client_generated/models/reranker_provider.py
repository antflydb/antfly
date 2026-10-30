from enum import StrEnum


class RerankerProvider(StrEnum):
    ANTFLY = "antfly"
    CLOUDFLARE = "cloudflare"
    COHERE = "cohere"
    VERTEX = "vertex"

    def __str__(self) -> str:
        return str(self.value)
