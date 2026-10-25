from enum import StrEnum


class GoogleEmbedderConfigRequestFormat(StrEnum):
    BATCH_EMBED_CONTENTS = "batch_embed_contents"
    EMBED_CONTENT = "embed_content"

    def __str__(self) -> str:
        return str(self.value)
