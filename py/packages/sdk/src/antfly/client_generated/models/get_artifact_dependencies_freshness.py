from enum import StrEnum


class GetArtifactDependenciesFreshness(StrEnum):
    ANY = "any"
    REQUIRE_CURRENT = "require_current"

    def __str__(self) -> str:
        return str(self.value)
