from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="ValueSearchExecution")


@_attrs_define
class ValueSearchExecution:
    """Execution policy for logical string-value operations. Limits apply to
    verification work per shard request, including repeated leaf evaluations.
    Exhaustion fails explicitly; it never returns a successful partial result.

        Attributes:
            allow_scan (bool | Unset): Permit logical value verification without an indexed candidate constraint. Default:
                False.
            max_documents (int | Unset):  Default: 100000.
            max_bytes (int | Unset):  Default: 67108864.
            max_regex_work (int | Unset): Bound on the sum of original string bytes multiplied by compiled regex states.
                Charged before regex evaluation; sensitive literals bypass this work. Default: 100000000.
    """

    allow_scan: bool | Unset = False
    max_documents: int | Unset = 100000
    max_bytes: int | Unset = 67108864
    max_regex_work: int | Unset = 100000000

    def to_dict(self) -> dict[str, Any]:
        allow_scan = self.allow_scan

        max_documents = self.max_documents

        max_bytes = self.max_bytes

        max_regex_work = self.max_regex_work

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if allow_scan is not UNSET:
            field_dict["allow_scan"] = allow_scan
        if max_documents is not UNSET:
            field_dict["max_documents"] = max_documents
        if max_bytes is not UNSET:
            field_dict["max_bytes"] = max_bytes
        if max_regex_work is not UNSET:
            field_dict["max_regex_work"] = max_regex_work

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        allow_scan = d.pop("allow_scan", UNSET)

        max_documents = d.pop("max_documents", UNSET)

        max_bytes = d.pop("max_bytes", UNSET)

        max_regex_work = d.pop("max_regex_work", UNSET)

        value_search_execution = cls(
            allow_scan=allow_scan,
            max_documents=max_documents,
            max_bytes=max_bytes,
            max_regex_work=max_regex_work,
        )

        return value_search_execution
