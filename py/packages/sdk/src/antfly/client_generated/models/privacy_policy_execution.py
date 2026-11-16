from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="PrivacyPolicyExecution")


@_attrs_define
class PrivacyPolicyExecution:
    """
    Attributes:
        batch_items (int | Unset):  Default: 8.
        max_attempts (int | Unset):  Default: 3.
    """

    batch_items: int | Unset = 8
    max_attempts: int | Unset = 3

    def to_dict(self) -> dict[str, Any]:
        batch_items = self.batch_items

        max_attempts = self.max_attempts

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if batch_items is not UNSET:
            field_dict["batch_items"] = batch_items
        if max_attempts is not UNSET:
            field_dict["max_attempts"] = max_attempts

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        batch_items = d.pop("batch_items", UNSET)

        max_attempts = d.pop("max_attempts", UNSET)

        privacy_policy_execution = cls(
            batch_items=batch_items,
            max_attempts=max_attempts,
        )

        return privacy_policy_execution
