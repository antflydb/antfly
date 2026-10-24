from enum import StrEnum


class BedrockGeneratorConfigProvider(StrEnum):
    BEDROCK = "bedrock"

    def __str__(self) -> str:
        return str(self.value)
