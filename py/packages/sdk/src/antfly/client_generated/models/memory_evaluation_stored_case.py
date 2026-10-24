from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_evaluation_case import MemoryEvaluationCase


T = TypeVar("T", bound="MemoryEvaluationStoredCase")


@_attrs_define
class MemoryEvaluationStoredCase:
    """
    Attributes:
        value (MemoryEvaluationCase):
        dataset (str):
        revision (int):
        owner (str):
        branch (str):
        recorded_at (int):
        fingerprint (str):
        forgotten (bool | Unset):  Default: False.
    """

    value: MemoryEvaluationCase
    dataset: str
    revision: int
    owner: str
    branch: str
    recorded_at: int
    fingerprint: str
    forgotten: bool | Unset = False

    def to_dict(self) -> dict[str, Any]:
        value = self.value.to_dict()

        dataset = self.dataset

        revision = self.revision

        owner = self.owner

        branch = self.branch

        recorded_at = self.recorded_at

        fingerprint = self.fingerprint

        forgotten = self.forgotten

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "value": value,
                "dataset": dataset,
                "revision": revision,
                "owner": owner,
                "branch": branch,
                "recorded_at": recorded_at,
                "fingerprint": fingerprint,
            }
        )
        if forgotten is not UNSET:
            field_dict["forgotten"] = forgotten

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_evaluation_case import MemoryEvaluationCase

        d = dict(src_dict)
        value = MemoryEvaluationCase.from_dict(d.pop("value"))

        dataset = d.pop("dataset")

        revision = d.pop("revision")

        owner = d.pop("owner")

        branch = d.pop("branch")

        recorded_at = d.pop("recorded_at")

        fingerprint = d.pop("fingerprint")

        forgotten = d.pop("forgotten", UNSET)

        memory_evaluation_stored_case = cls(
            value=value,
            dataset=dataset,
            revision=revision,
            owner=owner,
            branch=branch,
            recorded_at=recorded_at,
            fingerprint=fingerprint,
            forgotten=forgotten,
        )

        return memory_evaluation_stored_case
