from enum import StrEnum


class DeciderConfigProvider(StrEnum):
    ANTFLY = "antfly"
    CLOUDFLARE = "cloudflare"
    JEV = "jev"
    OPENAI = "openai"

    def __str__(self) -> str:
        return str(self.value)
