from enum import StrEnum


class IndexMutationConflictErrorError(StrEnum):
    ARTIFACT_DEPENDENCY_CONFLICT = "artifact_dependency_conflict"
    METADATA_MUTATION_OUTCOME_UNKNOWN = "metadata_mutation_outcome_unknown"
    TABLE_MUTATION_CONFLICT = "table_mutation_conflict"

    def __str__(self) -> str:
        return str(self.value)
