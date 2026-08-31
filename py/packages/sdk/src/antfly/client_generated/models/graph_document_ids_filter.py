from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

T = TypeVar("T", bound="GraphDocumentIdsFilter")


@_attrs_define
class GraphDocumentIdsFilter:
    """
    Attributes:
        ids (list[str]):
    """

    ids: list[str]

    def to_dict(self) -> dict[str, Any]:
        ids = self.ids

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "ids": ids,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        ids = cast(list[str], d.pop("ids"))

        graph_document_ids_filter = cls(
            ids=ids,
        )

        return graph_document_ids_filter
