from enum import StrEnum


class ResolverMutationResultReplaySequenceKind(StrEnum):
    RESOLVER_REVISION = "resolver_revision"
    SOURCE_JOURNAL = "source_journal"

    def __str__(self) -> str:
        return str(self.value)
