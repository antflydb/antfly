from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class LookupState(Enum):
    MISSING = "missing"
    PRESENT = "present"


class RegistryError(RuntimeError):
    """A registry could not provide an authoritative answer."""


@dataclass(frozen=True)
class Lookup:
    state: LookupState
    digest: str | None = None

    @classmethod
    def missing(cls) -> "Lookup":
        return cls(LookupState.MISSING)

    @classmethod
    def present(cls, digest: str) -> "Lookup":
        return cls(LookupState.PRESENT, digest)
