from enum import StrEnum


class InvokeInferenceConnectionOperation(StrEnum):
    CHUNK = "chunk"
    DICTATE = "dictate"
    EMBED = "embed"
    EXTRACT = "extract"
    GENERATE = "generate"
    READ = "read"
    RERANK = "rerank"
    REWRITE = "rewrite"

    def __str__(self) -> str:
        return str(self.value)
