from enum import StrEnum


class BedrockEmbedderConfigProvider(StrEnum):
    BEDROCK = "bedrock"

    def __str__(self) -> str:
        return str(self.value)
