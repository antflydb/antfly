from enum import StrEnum


class CloudflareRerankerConfigProvider(StrEnum):
    CLOUDFLARE = "cloudflare"

    def __str__(self) -> str:
        return str(self.value)
