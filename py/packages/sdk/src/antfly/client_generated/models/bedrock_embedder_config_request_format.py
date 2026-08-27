from enum import Enum


class BedrockEmbedderConfigRequestFormat(str, Enum):
    AUTO = "auto"
    COHERE_V3 = "cohere_v3"
    COHERE_V4 = "cohere_v4"
    TITAN_MULTIMODAL = "titan_multimodal"
    TITAN_TEXT = "titan_text"

    def __str__(self) -> str:
        return str(self.value)
