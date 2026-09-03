from enum import Enum


class BedrockEmbedderConfigProvider(str, Enum):
    BEDROCK = "bedrock"

    def __str__(self) -> str:
        return str(self.value)
