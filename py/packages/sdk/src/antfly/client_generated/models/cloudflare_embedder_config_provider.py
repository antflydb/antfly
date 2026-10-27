from enum import StrEnum


class CloudflareEmbedderConfigProvider(StrEnum):
    CLOUDFLARE = "cloudflare"

    def __str__(self) -> str:
        return str(self.value)
