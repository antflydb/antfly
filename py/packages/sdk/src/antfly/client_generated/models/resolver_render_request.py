from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="ResolverRenderRequest")


@_attrs_define
class ResolverRenderRequest:
    """
    Attributes:
        text (str):
        label (str):
        local_id (str | Unset):  Default: 'query'.
    """

    text: str
    label: str
    local_id: str | Unset = "query"

    def to_dict(self) -> dict[str, Any]:
        text = self.text

        label = self.label

        local_id = self.local_id

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "text": text,
                "label": label,
            }
        )
        if local_id is not UNSET:
            field_dict["local_id"] = local_id

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        text = d.pop("text")

        label = d.pop("label")

        local_id = d.pop("local_id", UNSET)

        resolver_render_request = cls(
            text=text,
            label=label,
            local_id=local_id,
        )

        return resolver_render_request
