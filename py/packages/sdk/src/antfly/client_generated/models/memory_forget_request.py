from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="MemoryForgetRequest")


@_attrs_define
class MemoryForgetRequest:
    """
    Attributes:
        evidence_ids (list[str] | Unset):
        assertion_ids (list[str] | Unset):
        source_ids (list[str] | Unset):
        all_ (bool | Unset):  Default: False.
    """

    evidence_ids: list[str] | Unset = UNSET
    assertion_ids: list[str] | Unset = UNSET
    source_ids: list[str] | Unset = UNSET
    all_: bool | Unset = False

    def to_dict(self) -> dict[str, Any]:
        evidence_ids: list[str] | Unset = UNSET
        if not isinstance(self.evidence_ids, Unset):
            evidence_ids = self.evidence_ids

        assertion_ids: list[str] | Unset = UNSET
        if not isinstance(self.assertion_ids, Unset):
            assertion_ids = self.assertion_ids

        source_ids: list[str] | Unset = UNSET
        if not isinstance(self.source_ids, Unset):
            source_ids = self.source_ids

        all_ = self.all_

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if evidence_ids is not UNSET:
            field_dict["evidence_ids"] = evidence_ids
        if assertion_ids is not UNSET:
            field_dict["assertion_ids"] = assertion_ids
        if source_ids is not UNSET:
            field_dict["source_ids"] = source_ids
        if all_ is not UNSET:
            field_dict["all"] = all_

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        evidence_ids = cast(list[str], d.pop("evidence_ids", UNSET))

        assertion_ids = cast(list[str], d.pop("assertion_ids", UNSET))

        source_ids = cast(list[str], d.pop("source_ids", UNSET))

        all_ = d.pop("all", UNSET)

        memory_forget_request = cls(
            evidence_ids=evidence_ids,
            assertion_ids=assertion_ids,
            source_ids=source_ids,
            all_=all_,
        )

        return memory_forget_request
