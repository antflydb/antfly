from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="GeneratedExtractionInput")


@_attrs_define
class GeneratedExtractionInput:
    """
    Attributes:
        content (str):
        id (str | Unset):
    """

    content: str
    id: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        content = self.content

        id = self.id

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "content": content,
            }
        )
        if id is not UNSET:
            field_dict["id"] = id

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        content = d.pop("content")

        id = d.pop("id", UNSET)

        generated_extraction_input = cls(
            content=content,
            id=id,
        )

        return generated_extraction_input
