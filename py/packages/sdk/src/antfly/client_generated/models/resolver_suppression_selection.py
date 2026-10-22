from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="ResolverSuppressionSelection")


@_attrs_define
class ResolverSuppressionSelection:
    """
    Attributes:
        keys (list[str] | Unset):
        texts (list[str] | Unset):
    """

    keys: list[str] | Unset = UNSET
    texts: list[str] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        keys: list[str] | Unset = UNSET
        if not isinstance(self.keys, Unset):
            keys = self.keys

        texts: list[str] | Unset = UNSET
        if not isinstance(self.texts, Unset):
            texts = self.texts

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if keys is not UNSET:
            field_dict["keys"] = keys
        if texts is not UNSET:
            field_dict["texts"] = texts

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        keys = cast(list[str], d.pop("keys", UNSET))

        texts = cast(list[str], d.pop("texts", UNSET))

        resolver_suppression_selection = cls(
            keys=keys,
            texts=texts,
        )

        return resolver_suppression_selection
