from enum import StrEnum


class InvokeInferenceConnectionOperation(StrEnum):
    CHUNK = "chunk"
    EMBED = "embed"
    EXTRACT = "extract"
    GENERATE = "generate"
    READ = "read"
    RERANK = "rerank"
    REWRITE = "rewrite"
    TRANSCRIBE = "transcribe"

    def __str__(self) -> str:
        return str(self.value)
