from enum import StrEnum


class CloudflareSTTConfigProvider(StrEnum):
    CLOUDFLARE = "cloudflare"

    def __str__(self) -> str:
        return str(self.value)
