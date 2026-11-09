from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="MemoryUtility")


@_attrs_define
class MemoryUtility:
    """
    Attributes:
        successes (int | Unset):  Default: 0.
        failures (int | Unset):  Default: 0.
        score (float | Unset):  Default: 0.0.
        complete (bool | Unset):  Default: True.
    """

    successes: int | Unset = 0
    failures: int | Unset = 0
    score: float | Unset = 0.0
    complete: bool | Unset = True

    def to_dict(self) -> dict[str, Any]:
        successes = self.successes

        failures = self.failures

        score = self.score

        complete = self.complete

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if successes is not UNSET:
            field_dict["successes"] = successes
        if failures is not UNSET:
            field_dict["failures"] = failures
        if score is not UNSET:
            field_dict["score"] = score
        if complete is not UNSET:
            field_dict["complete"] = complete

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        successes = d.pop("successes", UNSET)

        failures = d.pop("failures", UNSET)

        score = d.pop("score", UNSET)

        complete = d.pop("complete", UNSET)

        memory_utility = cls(
            successes=successes,
            failures=failures,
            score=score,
            complete=complete,
        )

        return memory_utility
