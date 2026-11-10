from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="PrivacyCategory")


@_attrs_define
class PrivacyCategory:
    """
    Attributes:
        name (str):
        description (str | Unset):
        threshold (float | Unset):
    """

    name: str
    description: str | Unset = UNSET
    threshold: float | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        description = self.description

        threshold = self.threshold

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
            }
        )
        if description is not UNSET:
            field_dict["description"] = description
        if threshold is not UNSET:
            field_dict["threshold"] = threshold

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        description = d.pop("description", UNSET)

        threshold = d.pop("threshold", UNSET)

        privacy_category = cls(
            name=name,
            description=description,
            threshold=threshold,
        )

        return privacy_category
