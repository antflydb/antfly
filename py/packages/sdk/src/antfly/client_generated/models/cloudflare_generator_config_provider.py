from enum import StrEnum


class CloudflareGeneratorConfigProvider(StrEnum):
    CLOUDFLARE = "cloudflare"

    def __str__(self) -> str:
        return str(self.value)
